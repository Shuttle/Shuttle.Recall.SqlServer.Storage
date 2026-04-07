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
        public RecallBuilder UseSqlServerEventStorage(Action<SqlServerStorageOptions>? configureOptions = null)
        {
            var services = recallBuilder.Services;

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

                options.UseSqlServer(sqlServerStorageOptions.ConnectionString, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout((int)sqlServerStorageOptions.CommandTimeout.TotalSeconds);
                });
            });

            return recallBuilder;
        }
    }
}