using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Pipelines.Logging;
using Shuttle.Recall.Testing;

namespace Shuttle.Recall.SqlServer.Storage.Tests;

public class StorageFixture : RecallFixture
{
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_exercise_event_store_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<StorageFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        var fixtureOptions = new RecallFixtureOptions(services)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.UseSqlServerEventStorage(builder =>
                {
                    builder.Options.ConnectionString = configuration.GetConnectionString("Recall") ?? throw new ApplicationException("A 'ConnectionString' with name 'Recall' is required which points to a Sql Server database.");
                    builder.Options.Schema = "recall_fixture";
                });
            })
            .WithStarting(async serviceProvider =>
            {
                var options = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;

                var knownAggregateIds = string.Join(',', KnownAggregateIds.Select(id => $"'{id}'"));

                using var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();

                await using var dbContext = scope.ServiceProvider.GetRequiredService<SqlServerStorageDbContext>();
#pragma warning disable EF1002
                await dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{options.Schema}].[PrimitiveEvent] WHERE Id IN ({knownAggregateIds})");
#pragma warning restore EF1002
            });

        await ExerciseStorageAsync(fixtureOptions, isTransactional);
    }


    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_exercise_sequencer_async(bool isTransactional)
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<StorageFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddPipelineLogging();

        await ExercisePrimitiveEventSequencerAsync(new RecallFixtureOptions(services)
            .WithAddRecall(recallBuilder =>
            {
                recallBuilder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(25)];

                recallBuilder.UseSqlServerEventStorage(builder =>
                {
                    builder.Options.ConnectionString = configuration.GetConnectionString("Recall") ?? throw new ApplicationException("A 'ConnectionString' with name 'Recall' is required which points to a Sql Server database.");
                    builder.Options.Schema = "recall_fixture";
                });
            }), isTransactional);
    }
}