using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.Storage;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class PrimitiveEventRepository(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, SqlServerStorageDbContext dbContext, IEventTypeRepository eventTypeRepository)
    : IPrimitiveEventRepository
{
    private readonly SqlServerStorageDbContext _dbContext = Guard.AgainstNull(dbContext);
    private readonly IEventTypeRepository _eventTypeRepository = Guard.AgainstNull(eventTypeRepository);
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);

    public async Task<IEnumerable<PrimitiveEvent>> GetAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var connection = _dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = $@"
SELECT 
    pe.Id, 
    pe.Version, 
    pe.EventId, 
    pe.EventEnvelope, 
    pe.SequenceNumber, 
    pe.RecordedAt, 
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

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new()
            {
                Id = reader.GetGuid(0),
                Version = reader.GetInt32(1),
                EventId = reader.GetGuid(2),
                EventEnvelope = (byte[])reader[3],
                SequenceNumber = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                RecordedAt = reader.GetFieldValue<DateTimeOffset>(5),
                CorrelationId = reader.IsDBNull(6) ? null : reader.GetGuid(6),
                EventType = reader.GetString(7)
            });
        }

        return result;
    }

    public async Task RemoveAsync(PrimitiveEvent.Specification specification, CancellationToken cancellationToken = default)
    {
        var eventTypeIds = new List<Guid>();

        foreach (var eventType in specification.EventTypes)
        {
            eventTypeIds.Add(await _eventTypeRepository.GetIdAsync(eventType, cancellationToken));
        }

        await _dbContext.Database.ExecuteSqlRawAsync(@$"
DELETE FROM [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]
FROM 
	[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] es
WHERE 
(
    @SequenceNumberStart = 0
    OR
	es.SequenceNumber >= @SequenceNumberStart
)
AND
(
    @SequenceNumberEnd = 0
    OR
	es.SequenceNumber <= @SequenceNumberEnd
)
{(
    !eventTypeIds.Any()
        ? string.Empty
        : $"AND es.EventTypeId IN ({string.Join(",", eventTypeIds.Select(id => string.Concat("'", id, "'")).ToArray())})"
)}
{(
    !specification.HasIds
        ? string.Empty
        : $"AND es.Id IN ({string.Join(",", specification.Ids.Select(id => string.Concat("'", id, "'")).ToArray())})"
)}
{(
    !specification.HasVersions
        ? string.Empty
        : $"AND es.[Version] IN ({string.Join(",", specification.Versions).ToArray()})"
)}
{(
    !specification.HasSequenceNumbers
        ? string.Empty
        : $"AND es.SequenceNumber IN ({string.Join(",", specification.SequenceNumbers)})"
)}
",
            [
                new SqlParameter("@SequenceNumberStart", specification.SequenceNumberStart),
                new SqlParameter("@SequenceNumberEnd", specification.SequenceNumberEnd)
            ],
            cancellationToken
        );
    }

    public async Task SaveAsync(IEnumerable<PrimitiveEvent> primitiveEvents, CancellationToken cancellationToken = default)
    {
        foreach (var primitiveEvent in primitiveEvents)
        {
            var eventTypeId = await _eventTypeRepository.GetIdAsync(primitiveEvent.EventType, cancellationToken).ConfigureAwait(false);

            await _dbContext.Database.ExecuteSqlRawAsync(@$"
INSERT INTO [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] 
(
    Id, 
    Version, 
    EventEnvelope, 
    EventId, 
    EventTypeId, 
    SequenceNumber, 
    RecordedAt, 
    CorrelationId
)
VALUES 
(
    @Id, 
    @Version, 
    @EventEnvelope, 
    @EventId, 
    @EventTypeId, 
    @SequenceNumber, 
    @RecordedAt, 
    @CorrelationId
)",
                [
                    new SqlParameter("@Id", primitiveEvent.Id),
                    new SqlParameter("@Version", primitiveEvent.Version),
                    new SqlParameter("@EventEnvelope", primitiveEvent.EventEnvelope),
                    new SqlParameter("@EventId", primitiveEvent.EventId),
                    new SqlParameter("@EventTypeId", eventTypeId),
                    new SqlParameter("@SequenceNumber", (object?)primitiveEvent.SequenceNumber ?? DBNull.Value),
                    new SqlParameter("@RecordedAt", primitiveEvent.RecordedAt),
                    new SqlParameter("@CorrelationId", (object?)primitiveEvent.CorrelationId ?? DBNull.Value)
                ],
                cancellationToken
            );
        }
    }
}