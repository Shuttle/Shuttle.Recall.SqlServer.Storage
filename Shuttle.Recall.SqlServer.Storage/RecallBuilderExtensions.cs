using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Shuttle.Recall.SqlServer.Storage;

public static class RecallBuilderExtensions
{
    extension(RecallBuilder recallBuilder)
    {
        public RecallBuilder UseSqlServerEventStorage(Action<SqlServerStorageOptions>? configureOptions = null)
        {
            var services = recallBuilder.Services;

            services.AddOptions<SqlServerStorageOptions>().Configure(options =>
            {
                configureOptions?.Invoke(options);
            });

            services.AddKeyedScoped<DbConnection>(SqlServerStorageDefaults.DbConnectionServiceKey, (serviceProvider, _) =>
                new SqlConnection(serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value.ConnectionString));
            
            services.AddSingleton<IValidateOptions<SqlServerStorageOptions>, SqlServerStorageOptionsValidator>();
            services.AddScoped<IPrimitiveEventQuery, PrimitiveEventQuery>();
            services.AddScoped<IPrimitiveEventRepository, PrimitiveEventRepository>();
            services.AddScoped<IEventTypeRepository, EventTypeRepository>();
            services.AddScoped<IIdKeyRepository, IdKeyRepository>();
            services.AddScoped<IPrimitiveEventSequencer, PrimitiveEventSequencer>();
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SqlServerStorageHostedService>());

            services.AddDbContext<SqlServerStorageDbContext>((serviceProvider, options) =>
            {
                var sqlServerStorageOptions = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;
                var dbConnection = serviceProvider.GetRequiredKeyedService<DbConnection>(sqlServerStorageOptions.DbConnectionServiceKey);

                options.UseSqlServer(dbConnection, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout((int)sqlServerStorageOptions.CommandTimeout.TotalSeconds);
                });
            });

            return recallBuilder;
        }
    }
}