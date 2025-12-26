using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.Storage;

public class EventTypeRepository(IDbContextFactory<SqlServerStorageDbContext> dbContextFactory) : IEventTypeRepository
{
    private readonly IDbContextFactory<SqlServerStorageDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly Dictionary<string, Guid> _cache = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public async Task<Guid> GetIdAsync(string typeName, CancellationToken cancellationToken = default)
    {
        Guard.AgainstEmpty(typeName);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            var key = typeName.ToLower();

            if (!_cache.ContainsKey(key))
            {
                await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

                var connection = dbContext.Database.GetDbConnection();

                await using var command = connection.CreateCommand();

                command.CommandText = @"
IF NOT EXISTS (SELECT 1 FROM EventTypes WHERE TypeName = @TypeName)
BEGIN
    INSERT INTO EventTypes (Id, TypeName) 
    VALUES (@Id, @TypeName);
END
SELECT Id FROM EventTypes WHERE TypeName = @TypeName;
";

                command.Parameters.Add(new SqlParameter("@TypeName", typeName));
                command.Parameters.Add(new SqlParameter("@Id", Guid.NewGuid()));

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    await connection.OpenAsync(cancellationToken);
                }

                var result = await command.ExecuteScalarAsync(cancellationToken);

                if (result != null)
                {
                    _cache.Add(key, (Guid)result);
                }
            }

            return _cache[key];
        }
        finally
        {
            _lock.Release();
        }
    }
}