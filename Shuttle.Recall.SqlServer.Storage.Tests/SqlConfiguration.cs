using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Pipelines.Logging;

namespace Shuttle.Recall.SqlServer.Storage.Tests;

[SetUpFixture]
public class SqlConfiguration
{
    public static IServiceCollection GetServiceCollection(IServiceCollection? serviceCollection = null)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddUserSecrets<SqlConfiguration>()
            .Build();

        var services = (serviceCollection ?? new ServiceCollection())
            .AddSingleton<IConfiguration>(configuration)
            .AddSqlServerEventStorage(builder =>
            {
                configuration.GetSection(SqlServerStorageOptions.SectionName).Bind(builder.Options);

                builder.Options.ConnectionString = configuration.GetConnectionString("Recall") ?? throw new ApplicationException("A 'ConnectionString' with name 'Recall' is required which points to a Sql Server database.");
                builder.Options.Schema = "RecallFixture";
            })
            .AddPipelineLogging(); ;

        return services;
    }
}