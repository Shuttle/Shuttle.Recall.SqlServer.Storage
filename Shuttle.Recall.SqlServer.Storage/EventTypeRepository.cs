using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;

namespace Shuttle.Recall.SqlServer.Storage;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class EventTypeRepository(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, SqlServerStorageDbContext dbContext) : IEventTypeRepository
{
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);
    private readonly SqlServerStorageDbContext _dbContext = Guard.AgainstNull(dbContext);
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
                var connection = _dbContext.Database.GetDbConnection();

                await using var command = connection.CreateCommand();

                command.CommandText = @$"
IF NOT EXISTS (SELECT 1 FROM [{_sqlServerStorageOptions.Schema}].[EventType] WHERE TypeName = @TypeName)
BEGIN
    INSERT INTO [{_sqlServerStorageOptions.Schema}].[EventType] 
    (
        Id, 
        TypeName
    ) 
    VALUES 
    (
        @Id, 
        @TypeName
    );
END
SELECT Id FROM [{_sqlServerStorageOptions.Schema}].[EventType] WHERE TypeName = @TypeName;
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