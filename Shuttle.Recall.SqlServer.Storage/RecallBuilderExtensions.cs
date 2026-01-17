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
        public RecallBuilder UseSqlServerEventStorage(Action<SqlServerStorageBuilder>? builder = null)
        {
            var services = recallBuilder.Services;
            var sqlServerStorageBuilder = new SqlServerStorageBuilder(services);

            builder?.Invoke(sqlServerStorageBuilder);

            services.TryAddSingleton<IValidateOptions<SqlServerStorageOptions>, SqlServerStorageOptionsValidator>();
            services.TryAddScoped<IPrimitiveEventQuery, PrimitiveEventQuery>();
            services.TryAddScoped<IPrimitiveEventRepository, PrimitiveEventRepository>();
            services.TryAddScoped<IEventTypeRepository, EventTypeRepository>();
            services.TryAddScoped<IIdKeyRepository, IdKeyRepository>();
            services.TryAddScoped<IPrimitiveEventSequencer, PrimitiveEventSequencer>();
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SqlServerStorageHostedService>());

            services.AddOptions<SqlServerStorageOptions>().Configure(options =>
            {
                options.ConnectionString = sqlServerStorageBuilder.Options.ConnectionString;
                options.Schema = sqlServerStorageBuilder.Options.Schema;
                options.CommandTimeout = sqlServerStorageBuilder.Options.CommandTimeout;
                options.PrimitiveEventSequencerLimit = sqlServerStorageBuilder.Options.PrimitiveEventSequencerLimit < 1 ? 1 : sqlServerStorageBuilder.Options.PrimitiveEventSequencerLimit;
            });

            services.AddDbContext<SqlServerStorageDbContext>((sp, options) =>
            {
                var dbConnection = sp.GetService<DbConnection>();

                if (dbConnection != null)
                {
                    var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(sqlServerStorageBuilder.Options.ConnectionString);

                    if (!dbConnection.Database.Equals(sqlConnectionStringBuilder.InitialCatalog, StringComparison.InvariantCultureIgnoreCase) ||
                        !dbConnection.DataSource.Equals(sqlConnectionStringBuilder.DataSource, StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new ApplicationException(Resources.DbConnectionException);
                    }

                    options.UseSqlServer(dbConnection, Configure);
                }
                else
                {
                    options.UseSqlServer(sqlServerStorageBuilder.Options.ConnectionString, Configure);
                }
            });

            return recallBuilder;

            void Configure(SqlServerDbContextOptionsBuilder sqlServerOptions)
            {
                sqlServerOptions.CommandTimeout(sqlServerStorageBuilder.Options.CommandTimeout.Seconds);
            }
        }
    }
}