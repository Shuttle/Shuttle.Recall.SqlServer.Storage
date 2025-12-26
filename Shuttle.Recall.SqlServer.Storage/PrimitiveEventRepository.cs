using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;

namespace Shuttle.Recall.SqlServer.Storage;

public class PrimitiveEventRepository(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IDbContextFactory<SqlServerStorageDbContext> dbContextFactory, IEventTypeRepository eventTypeRepository)
    : IPrimitiveEventRepository
{
    private readonly IDbContextFactory<SqlServerStorageDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly IEventTypeRepository _eventTypeRepository = Guard.AgainstNull(eventTypeRepository);
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);

    public async Task<IEnumerable<PrimitiveEvent>> GetAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = $@"
SELECT 
    pe.Id, 
    pe.Version, 
    pe.EventId, 
    pe.EventEnvelope, 
    pe.SequenceNumber, 
    pe.DateRegistered, 
    pe.CorrelationId, 
    et.TypeName
FROM 
    [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] pe
INNER JOIN 
    [{_sqlServerStorageOptions.Schema}].[EventType] et ON pe.EventTypeId = et.Id
WHERE 
    pe.Id = @Id
ORDER BY 
    pe.Version ASC
";

        command.Parameters.Add(new SqlParameter("@Id", id));

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        var result = new List<PrimitiveEvent>();

        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                result.Add(new()
                {
                    Id = reader.GetGuid(0),
                    Version = reader.GetInt32(1),
                    EventId = reader.GetGuid(2),
                    EventEnvelope = (byte[])reader[3],
                    SequenceNumber = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    DateRegistered = reader.GetDateTime(5),
                    CorrelationId = reader.IsDBNull(6) ? null : reader.GetGuid(6),
                    EventType = reader.GetString(7)
                });
            }
        }

        return result;
    }

    public async ValueTask<long> GetMaxSequenceNumberAsync(CancellationToken cancellationToken = default)
    {
        var result = await ExecuteScalarAsync(
            $"SELECT ISNULL(MAX(SequenceNumber), 0) FROM [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]",
            cancellationToken
        );

        return Convert.ToInt64(result);
    }

    public async ValueTask<long> GetSequenceNumberAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var result = await ExecuteScalarAsync(
            $"SELECT ISNULL(MAX(SequenceNumber), 0) FROM [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] WHERE EventId = @Id",
            cancellationToken,
            new SqlParameter("@Id", id)
        );

        return Convert.ToInt64(result);
    }

    public async Task RemoveAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await ExecuteNonQueryAsync(
            $"DELETE FROM [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] WHERE Id = @Id",
            cancellationToken,
            new SqlParameter("@Id", id)
        );
    }

    public async ValueTask<long> SaveAsync(IEnumerable<PrimitiveEvent> primitiveEvents, CancellationToken cancellationToken = default)
    {
        long maxSequenceNumber = 0;

        foreach (var primitiveEvent in primitiveEvents)
        {
            var eventTypeId = await _eventTypeRepository.GetIdAsync(primitiveEvent.EventType, cancellationToken).ConfigureAwait(false);

            await ExecuteNonQueryAsync($@"
                INSERT INTO [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] 
                (Id, Version, EventEnvelope, EventId, EventTypeId, SequenceNumber, DateRegistered, CorrelationId)
                VALUES (@Id, @Version, @EventEnvelope, @EventId, @EventTypeId, @SequenceNumber, @DateRegistered, @CorrelationId)",
                cancellationToken,
                new SqlParameter("@Id", primitiveEvent.Id),
                new SqlParameter("@Version", primitiveEvent.Version),
                new SqlParameter("@EventEnvelope", primitiveEvent.EventEnvelope),
                new SqlParameter("@EventId", primitiveEvent.EventId),
                new SqlParameter("@EventTypeId", eventTypeId),
                new SqlParameter("@SequenceNumber", (object?)primitiveEvent.SequenceNumber ?? DBNull.Value),
                new SqlParameter("@DateRegistered", primitiveEvent.DateRegistered),
                new SqlParameter("@CorrelationId", (object?)primitiveEvent.CorrelationId ?? DBNull.Value)
            );

            if (primitiveEvent.SequenceNumber.HasValue && primitiveEvent.SequenceNumber > maxSequenceNumber)
            {
                maxSequenceNumber = primitiveEvent.SequenceNumber.Value;
            }
        }

        return maxSequenceNumber;
    }

    private async Task ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken, params SqlParameter[] parameters)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = sql;
        command.Parameters.AddRange(parameters);

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        var currentTransaction = dbContext.Database.CurrentTransaction;
        if (currentTransaction != null)
        {
            command.Transaction = currentTransaction.GetDbTransaction();
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<object?> ExecuteScalarAsync(string sql, CancellationToken cancellationToken, params SqlParameter[] parameters)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = sql;
        command.Parameters.AddRange(parameters);

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        var currentTransaction = dbContext.Database.CurrentTransaction;
        if (currentTransaction != null)
        {
            command.Transaction = currentTransaction.GetDbTransaction();
        }

        return await command.ExecuteScalarAsync(cancellationToken);
    }
}