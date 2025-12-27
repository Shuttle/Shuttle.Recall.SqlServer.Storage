using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Microsoft.Data.SqlClient;

namespace Shuttle.Recall.SqlServer.Storage;

public class PrimitiveEventQuery(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IDbContextFactory<SqlServerStorageDbContext> dbContextFactory, IEventTypeRepository eventTypeRepository)
    : IPrimitiveEventQuery
{
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);
    private readonly IDbContextFactory<SqlServerStorageDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly IEventTypeRepository _eventTypeRepository = Guard.AgainstNull(eventTypeRepository);

    public async Task<IEnumerable<PrimitiveEvent>> SearchAsync(PrimitiveEvent.Specification specification, CancellationToken cancellationToken = default)
    {
        var eventTypeIds = new List<Guid>();

        foreach (var eventType in specification.EventTypes)
        {
            eventTypeIds.Add(await _eventTypeRepository.GetIdAsync(eventType, cancellationToken));
        }

        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var connection = dbContext.Database.GetDbConnection();

        await using var command = connection.CreateCommand();

        command.CommandText = $@"
SELECT {(specification.MaximumRows > 0 ? $"TOP {specification.MaximumRows}" : string.Empty)}
	es.[Id],
	es.[Version],
	es.[EventId],
	es.[EventEnvelope],
	es.[SequenceNumber],
	es.[DateRegistered],
	es.[CorrelationId],
	et.[TypeName] EventType
FROM 
	[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] es
INNER JOIN
	[{_sqlServerStorageOptions.Schema}].[EventType] et ON et.Id = es.EventTypeId
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
        : $"AND EventTypeId IN ({string.Join(",", eventTypeIds.Select(id => string.Concat("'", id, "'")).ToArray())})"
)}
{(
    !specification.HasIds
        ? string.Empty
        : $"AND Id IN ({string.Join(",", specification.Ids.Select(id => string.Concat("'", id, "'")).ToArray())})"
)}
{(
    !specification.HasSequenceNumbers
        ? string.Empty
        : $"AND SequenceNumber IN ({string.Join(",", specification.SequenceNumbers)})"
)}
ORDER BY
	es.SequenceNumber
";

        command.Parameters.Add(new SqlParameter("@SequenceNumberStart", specification.SequenceNumberStart));
        command.Parameters.Add(new SqlParameter("@SequenceNumberEnd", specification.SequenceNumberEnd));

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
                DateRegistered = reader.GetDateTime(5),
                CorrelationId = reader.IsDBNull(6) ? null : reader.GetGuid(6),
                EventType = reader.GetString(7)
            });
        }

        return result;
    }
}