using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.Storage.Database;

public class DatabaseMigrationService
{
    private readonly SqlServerStorageHostedService _hostedService;

    public DatabaseMigrationService(IOptions<RecallOptions> recallOptions, IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IServiceScopeFactory serviceScopeFactory)
    {
        Guard.AgainstNull(recallOptions);
        Guard.AgainstNull(sqlServerStorageOptions);
        Guard.AgainstNull(serviceScopeFactory);

        _hostedService = new SqlServerStorageHostedService(recallOptions, sqlServerStorageOptions, serviceScopeFactory);
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        await _hostedService.StartAsync(cancellationToken);
    }
}
