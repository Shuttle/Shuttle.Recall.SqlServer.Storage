using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.Storage;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddSqlServerEventStorage(Action<SqlServerStorageBuilder>? builder = null)
        {
            var sqlServerStorageBuilder = new SqlServerStorageBuilder(Guard.AgainstNull(services));

            builder?.Invoke(sqlServerStorageBuilder);

            services.AddSingleton<IValidateOptions<SqlServerStorageOptions>, SqlServerStorageOptionsValidator>();
            services.AddSingleton<IPrimitiveEventRepository, PrimitiveEventRepository>();
            services.AddSingleton<IEventTypeRepository, EventTypeRepository>();
            services.AddSingleton<IIdKeyRepository, IdKeyRepository>();
            services.AddSingleton<IPrimitiveEventSequencer, PrimitiveEventSequencer>();
            services.AddHostedService<SqlServerStorageHostedService>();

            services.AddOptions<SqlServerStorageOptions>().Configure(options =>
            {
                options.ConnectionString = sqlServerStorageBuilder.Options.ConnectionString;
                options.Schema = sqlServerStorageBuilder.Options.Schema;
                options.CommandTimeout = sqlServerStorageBuilder.Options.CommandTimeout;
            });

            services.AddDbContextFactory<SqlServerStorageDbContext>(dbContextFactoryBuilder =>
            {
                dbContextFactoryBuilder.UseSqlServer(sqlServerStorageBuilder.Options.ConnectionString, sqlServerOptions =>
                {
                    sqlServerOptions.CommandTimeout(sqlServerStorageBuilder.Options.CommandTimeout);
                });
            });

            return services;
        }
    }
}