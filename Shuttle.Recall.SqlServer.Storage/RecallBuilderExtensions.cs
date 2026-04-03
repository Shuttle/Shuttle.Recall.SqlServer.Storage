using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Data.Common;

namespace Shuttle.Recall.SqlServer.Storage;

public static class RecallBuilderExtensions
{
    extension(RecallBuilder recallBuilder)
    {
        public RecallBuilder UseSqlServerEventStorage(Action<SqlServerStorageOptions>? configureOptions = null)
        {
            var services = recallBuilder.Services;

            services.AddOptions();
            services.AddOptions<SqlServerStorageOptions>().Configure(options =>
            {
                configureOptions?.Invoke(options);
            });

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

                var dbConnection = serviceProvider.GetKeyedService<DbConnection>(sqlServerStorageOptions.DbConnectionServiceKey);

                if (dbConnection != null)
                {
                    var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(sqlServerStorageOptions.ConnectionString);

                    if (!dbConnection.Database.Equals(sqlConnectionStringBuilder.InitialCatalog, StringComparison.InvariantCultureIgnoreCase) ||
                        !dbConnection.DataSource.Equals(sqlConnectionStringBuilder.DataSource, StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new ApplicationException(Resources.DbConnectionException);
                    }

                    options.UseSqlServer(dbConnection, sqlServerOptions =>
                    {
                        sqlServerOptions.CommandTimeout(sqlServerStorageOptions.CommandTimeout.Seconds);
                    });
                }
                else
                {
                    options.UseSqlServer(sqlServerStorageOptions.ConnectionString, sqlServerOptions =>
                    {
                        sqlServerOptions.CommandTimeout(sqlServerStorageOptions.CommandTimeout.Seconds);
                    });
                }
            });

            return recallBuilder;
        }
    }
}