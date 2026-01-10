using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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
            services.TryAddSingleton<IPrimitiveEventQuery, PrimitiveEventQuery>();
            services.TryAddScoped<IPrimitiveEventRepository, PrimitiveEventRepository>();
            services.TryAddScoped<IEventTypeRepository, EventTypeRepository>();
            services.TryAddScoped<IIdKeyRepository, IdKeyRepository>();
            services.TryAddSingleton<IPrimitiveEventSequencer, PrimitiveEventSequencer>();
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SqlServerStorageHostedService>());

            services.AddOptions<SqlServerStorageOptions>().Configure(options =>
            {
                options.ConnectionString = sqlServerStorageBuilder.Options.ConnectionString;
                options.Schema = sqlServerStorageBuilder.Options.Schema;
                options.CommandTimeout = sqlServerStorageBuilder.Options.CommandTimeout;
                options.PrimitiveEventSequencerLimit = sqlServerStorageBuilder.Options.PrimitiveEventSequencerLimit < 1 ? 1 : sqlServerStorageBuilder.Options.PrimitiveEventSequencerLimit;
            });

            services.AddDbContextFactory<SqlServerStorageDbContext>(dbContextFactoryBuilder =>
            {
                dbContextFactoryBuilder.UseSqlServer(sqlServerStorageBuilder.Options.ConnectionString, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout(sqlServerStorageBuilder.Options.CommandTimeout.Seconds);
                });
            });

            return recallBuilder;
        }
    }
}