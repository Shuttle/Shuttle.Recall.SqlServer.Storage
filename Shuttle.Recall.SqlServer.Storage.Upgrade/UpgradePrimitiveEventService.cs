using System.Data;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using Serilog;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.Storage.Upgrade;

public class UpgradePrimitiveEventService(ILogger logger, string connectionString, string schema, long fromSequenceNumber)
{
    private readonly string _connectionString = Guard.AgainstEmpty(connectionString);
    private readonly string _schema = Guard.AgainstEmpty(schema);
    private readonly string _schemaUpgrade = $"{Guard.AgainstEmpty(schema)}_upgrade" ;

    private const int BatchSize = 1000;

    public async Task ExecuteAsync()
    {
        await using (var connection = new SqlConnection(Guard.AgainstEmpty(_connectionString)))
        {
            await connection.OpenAsync();

            await InitialDatabaseConfigurationAsync(connection);

            var sql = $@"
            SELECT 
                pe.Id,
                pe.CorrelationId,
                pe.EventId,
                pe.EventTypeId,
                pe.Version,
                pe.SequenceNumber,
                pe.DateRegistered,
                pe.EventEnvelope
            FROM [{_schema}].[PrimitiveEvent] pe
            WHERE pe.SequenceNumber >= @FromSequenceNumber
            ORDER BY pe.SequenceNumber ASC;";

            await using var command = new SqlCommand(sql, connection);

            command.Parameters.AddWithValue("@FromSequenceNumber", fromSequenceNumber);

            await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

            var batch = new List<(v20.PrimitiveEvent primitiveEvent, v20.EventEnvelope envelope)>();
            var processedCount = 0;

            while (await reader.ReadAsync())
            {
                var primitiveEvent = new v20.PrimitiveEvent
                {
                    Id = reader.GetGuid(reader.GetOrdinal("Id")),
                    CorrelationId = await reader.IsDBNullAsync(reader.GetOrdinal("CorrelationId"))
                        ? null
                        : reader.GetGuid(reader.GetOrdinal("CorrelationId")),
                    EventId = reader.GetGuid(reader.GetOrdinal("EventId")),
                    EventTypeId = reader.GetGuid(reader.GetOrdinal("EventTypeId")),
                    Version = reader.GetInt32(reader.GetOrdinal("Version")),
                    SequenceNumber = reader.GetInt64(reader.GetOrdinal("SequenceNumber")),
                    DateRegistered = reader.GetDateTime(reader.GetOrdinal("DateRegistered")),
                    EventEnvelope = reader.GetFieldValue<byte[]>(reader.GetOrdinal("EventEnvelope")) ?? Array.Empty<byte>()
                };

                v20.EventEnvelope? envelope;

                try
                {
                    envelope = JsonSerializer.Deserialize<v20.EventEnvelope>(primitiveEvent.EventEnvelope, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                    if (envelope == null)
                    {
                        throw new InvalidOperationException($"Deserialization returned null for PrimitiveEvent Id: {primitiveEvent.Id}, SequenceNumber: {primitiveEvent.SequenceNumber}");
                    }
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException($"Failed to deserialize EventEnvelope for PrimitiveEvent Id: {primitiveEvent.Id}, SequenceNumber: {primitiveEvent.SequenceNumber}", ex);
                }

                batch.Add((primitiveEvent, envelope));

                if (batch.Count >= BatchSize)
                {
                    await ProcessBatch(_connectionString, batch, _schemaUpgrade);
                    processedCount += batch.Count;
                    logger.Information($"Processed {processedCount} events...");
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await ProcessBatch(_connectionString, batch, _schemaUpgrade);
                processedCount += batch.Count;
                logger.Information($"Processed {processedCount} events...");
            }
        }

        await using (var connection = new SqlConnection(Guard.AgainstEmpty(_connectionString)))
        {
            await connection.OpenAsync();

            var transaction = await connection.BeginTransactionAsync();

            await CompletedDatabaseConfigurationAsync(connection);

            await transaction.CommitAsync();
        }
    }

    private async Task CompletedDatabaseConfigurationAsync(SqlConnection connection)
    {
        var command = connection.CreateCommand();

        command.CommandText = $@"
DECLARE @lock_result INT;
DECLARE @MaxSequenceNumber BIGINT;
DECLARE @MaxSequenceNumberBACKUP BIGINT;

EXEC @lock_result = sp_getapplock @Resource = '{typeof(UpgradePrimitiveEventService).FullName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

IF @lock_result < 0
    THROW 50000, 'Failed to acquire schema lock', 1;

BEGIN TRY
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_schemaUpgrade}].[PrimitiveEvent]') AND type in (N'U')) AND
        EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_schema}].[PrimitiveEvent]') AND type in (N'U'))
    BEGIN
        DROP TABLE [{_schema}].[PrimitiveEvent]
        ALTER SCHEMA [{_schemaUpgrade}] TRANSFER [{_schema}].[PrimitiveEvent];
        DROP SCHEMA [{_schemaUpgrade}]
    END

    ALTER TABLE [{_schema}].[PrimitiveEvent]
        ADD CONSTRAINT [DF_PrimitiveEvent_RecordedAt] DEFAULT (SYSUTCDATETIME()) FOR [RecordedAt];

    ALTER TABLE [{_schema}].[PrimitiveEvent] 
        WITH CHECK ADD CONSTRAINT [FK_PrimitiveEvent_EventType_EventTypeId] 
        FOREIGN KEY([EventTypeId]) REFERENCES [{_schema}].[EventType] ([Id]) ON DELETE CASCADE;

    ALTER TABLE [{_schema}].[PrimitiveEvent] 
        CHECK CONSTRAINT [FK_PrimitiveEvent_EventType_EventTypeId];

    CREATE NONCLUSTERED INDEX [IX_PrimitiveEvent_EventTypeId] 
        ON [{_schema}].[PrimitiveEvent] ([EventTypeId] ASC);

    CREATE UNIQUE NONCLUSTERED INDEX [IX_PrimitiveEvent_SequenceNumber] 
        ON [{_schema}].[PrimitiveEvent] ([SequenceNumber] ASC, [RecordedAt] ASC)
        WHERE [SequenceNumber] IS NOT NULL;

    CREATE NONCLUSTERED INDEX [IX_PrimitiveEvent_NullSequence_DateRegistered_Version] 
        ON [{_schema}].[PrimitiveEvent] ([RecordedAt] ASC, [Version] ASC)
        WHERE [SequenceNumber] IS NULL;
END TRY
BEGIN CATCH
    EXEC sp_releaseapplock @Resource = '{typeof(UpgradePrimitiveEventService).FullName}', @LockOwner = 'Session';
    THROW;
END CATCH

EXEC sp_releaseapplock @Resource = '{typeof(UpgradePrimitiveEventService).FullName}', @LockOwner = 'Session';
";

        await command.ExecuteNonQueryAsync();
    }

    private async Task InitialDatabaseConfigurationAsync(SqlConnection connection)
    {
        var command = connection.CreateCommand();

        command.CommandText = $@"
DECLARE @lock_result INT;
DECLARE @MaxSequenceNumber BIGINT;
DECLARE @MaxSequenceNumberBACKUP BIGINT;

EXEC @lock_result = sp_getapplock @Resource = '{typeof(UpgradePrimitiveEventService).FullName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

IF @lock_result < 0
    THROW 50000, 'Failed to acquire schema lock', 1;

BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_schemaUpgrade}')
    BEGIN
        EXEC('CREATE SCHEMA [{_schemaUpgrade}]');
    END

    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_schemaUpgrade}].[PrimitiveEvent]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [{_schemaUpgrade}].[PrimitiveEvent]
        (
            [Id] [uniqueidentifier] NOT NULL,
            [Version] [int] NOT NULL,
            [CorrelationId] [uniqueidentifier] NULL,
            [EventEnvelope] [varbinary](max) NOT NULL,
            [EventId] [uniqueidentifier] NOT NULL,
            [EventTypeId] [uniqueidentifier] NOT NULL,
            [RecordedAt] [datetimeoffset](7) NOT NULL,
            [SequenceNumber] [bigint] NULL,
            CONSTRAINT [PK_PrimitiveEvent] PRIMARY KEY CLUSTERED ([Id] ASC, [Version] ASC)
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
    END

    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{schema}].[PrimitiveEvent_BACKUP]') AND type in (N'U'))
    BEGIN
        SELECT * INTO [{schema}].[PrimitiveEvent_BACKUP] FROM [{schema}].[PrimitiveEvent]
    END
    ELSE
    BEGIN
        SELECT @MaxSequenceNumber = MAX([SequenceNumber]) FROM [{schema}].[PrimitiveEvent]
        SELECT @MaxSequenceNumberBACKUP = MAX([SequenceNumber]) FROM [{schema}].[PrimitiveEvent_BACKUP]

        IF (@MaxSequenceNumber <> @MaxSequenceNumberBACKUP)
        BEGIN
            TRUNCATE TABLE [{schema}].[PrimitiveEvent_BACKUP]
            SELECT * INTO [{schema}].[PrimitiveEvent_BACKUP] FROM [{schema}].[PrimitiveEvent]
        END
    END
END TRY
BEGIN CATCH
    EXEC sp_releaseapplock @Resource = '{typeof(UpgradePrimitiveEventService).FullName}', @LockOwner = 'Session';
    THROW;
END CATCH

EXEC sp_releaseapplock @Resource = '{typeof(UpgradePrimitiveEventService).FullName}', @LockOwner = 'Session';
";

        await command.ExecuteNonQueryAsync();
    }

    private async Task ProcessBatch(string connectionString, List<(v20.PrimitiveEvent primitiveEvent, v20.EventEnvelope envelope)> batch, string schemaUpgrade)
    {
        await using var connection = new SqlConnection(Guard.AgainstEmpty(connectionString));

        await connection.OpenAsync();

        foreach (var (primitiveEvent, envelope) in batch)
        {
            await UpgradeEvent(envelope, primitiveEvent, connection, schemaUpgrade);
        }
    }

    private async Task UpgradeEvent(v20.EventEnvelope envelope, v20.PrimitiveEvent primitiveEvent, SqlConnection destConnection, string schemaUpgrade)
    {
        var checkSql = $@"
            SELECT COUNT(1) 
            FROM [{schemaUpgrade}].[PrimitiveEvent]
            WHERE Id = @Id AND Version = @Version;";

        await using var checkCommand = new SqlCommand(checkSql, destConnection);

        checkCommand.Parameters.AddWithValue("@Id", primitiveEvent.Id);
        checkCommand.Parameters.AddWithValue("@Version", primitiveEvent.Version);

        var exists = (int)(await checkCommand.ExecuteScalarAsync() ?? 0) > 0;

        if (!exists)
        {
            var serializedEnvelope = JsonSerializer.SerializeToUtf8Bytes(new EventEnvelope
            {
                Event = envelope.Event,
                AssemblyQualifiedName = envelope.AssemblyQualifiedName,
                CompressionAlgorithm = envelope.CompressionAlgorithm,
                EncryptionAlgorithm = envelope.EncryptionAlgorithm,
                EventId = envelope.EventId,
                EventType = envelope.EventType,
                RecordedAt = envelope.EventDate,
                Version = envelope.Version,
                Headers = envelope.Headers
            });

            var insertSql = $@"
                INSERT INTO [{schemaUpgrade}].[PrimitiveEvent]
                (Id, CorrelationId, EventId, EventTypeId, Version, SequenceNumber, RecordedAt, EventEnvelope)
                VALUES 
                (@Id, @CorrelationId, @EventId, @EventTypeId, @Version, @SequenceNumber, @RecordedAt, @EventEnvelope);";

            await using var insertCommand = new SqlCommand(insertSql, destConnection);
            insertCommand.Parameters.AddWithValue("@Id", primitiveEvent.Id);

            insertCommand.Parameters.AddWithValue("@CorrelationId", (object?)primitiveEvent.CorrelationId ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("@EventId", primitiveEvent.EventId);
            insertCommand.Parameters.AddWithValue("@EventTypeId", primitiveEvent.EventTypeId);
            insertCommand.Parameters.AddWithValue("@Version", primitiveEvent.Version);
            insertCommand.Parameters.AddWithValue("@SequenceNumber", primitiveEvent.SequenceNumber);
            insertCommand.Parameters.AddWithValue("@RecordedAt", primitiveEvent.DateRegistered);
            insertCommand.Parameters.AddWithValue("@EventEnvelope", serializedEnvelope);

            await insertCommand.ExecuteNonQueryAsync();

            logger.Information($"[inserted] : id = '{primitiveEvent.Id}' / version = {envelope.Version} / event type = {envelope.EventType} / sequence number = {primitiveEvent.SequenceNumber}");

            Console.WriteLine();
        }
        else
        {
            logger.Information($"[skipped] : id = '{primitiveEvent.Id}' / version = {envelope.Version} / event type = {envelope.EventType} / sequence number = {primitiveEvent.SequenceNumber}");
        }
    }
}