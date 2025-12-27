using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.Storage;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class IdKeyRepository(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IDbContextFactory<SqlServerStorageDbContext> dbContextFactory) : IIdKeyRepository
{
    private readonly IDbContextFactory<SqlServerStorageDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);

    public async Task AddAsync(Guid id, string key, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        await dbContext.Database.ExecuteSqlRawAsync(@$"
INSERT INTO [{_sqlServerStorageOptions.Schema}].[IdKey]
(
    Id, 
    UniqueKey
) 
VALUES 
(
    @Id, 
@UniqueKey);
",
            [
                new SqlParameter("@Id", id),
                new SqlParameter("@UniqueKey", key)
            ],
            cancellationToken
        );
    }

    public async ValueTask<bool> ContainsAsync(string key, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await dbContext.Database.SqlQueryRaw<int>($"SELECT COUNT(1) [Value] FROM [{_sqlServerStorageOptions.Schema}].[IdKey] WHERE UniqueKey = @Key", new SqlParameter("@Key", key)).FirstOrDefaultAsync(cancellationToken) > 0;
    }

    public async ValueTask<bool> ContainsAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await dbContext.Database.SqlQueryRaw<int>($"SELECT COUNT(1) [Value] FROM [{_sqlServerStorageOptions.Schema}].[IdKey] WHERE Id = @Id", new SqlParameter("@Id", id)).FirstOrDefaultAsync(cancellationToken) > 0;
    }

    public async ValueTask<Guid?> FindAsync(string key, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await dbContext.Database.SqlQueryRaw<Guid?>($"SELECT Id [Value] FROM [{_sqlServerStorageOptions.Schema}].[IdKey] WHERE UniqueKey = @Key", new SqlParameter("@Key", key)).FirstOrDefaultAsync(cancellationToken);
    }

    public async Task RekeyAsync(string key, string rekey, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        await dbContext.Database.ExecuteSqlRawAsync(@$"
UPDATE 
    [{_sqlServerStorageOptions.Schema}].[IdKey] 
SET 
    UniqueKey = @NewKey 
WHERE 
    UniqueKey = @OldKey;",
            [
                new SqlParameter("@OldKey", key),
                new SqlParameter("@NewKey", rekey)
            ],
            cancellationToken);
    }

    public async Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        await dbContext.Database.ExecuteSqlRawAsync(@$"
DELETE FROM 
    [{_sqlServerStorageOptions.Schema}].[IdKey] 
WHERE 
    UniqueKey = @UniqueKey;",
            [
                new SqlParameter("@UniqueKey", key)
            ],
            cancellationToken);
    }

    public async Task RemoveAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        await dbContext.Database.ExecuteSqlRawAsync(@$"
DELETE FROM 
    [{_sqlServerStorageOptions.Schema}].[IdKey] 
WHERE 
    Id = @Id;",
            [
                new SqlParameter("@Id", id)
            ],
            cancellationToken);
    }
}