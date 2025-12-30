using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Recall.Testing;

namespace Shuttle.Recall.SqlServer.Storage.Tests;

public class StorageFixture : RecallFixture
{
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_exercise_event_store_async(bool isTransactional)
    {
        var services = SqlServerFixtureConfiguration.GetServiceCollection();

        var fixtureConfiguration = new FixtureConfiguration(services)
            .WithStarting(async serviceProvider =>
            {
                var dbContextFactory = serviceProvider.GetRequiredService<IDbContextFactory<SqlServerStorageDbContext>>();
                var options = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;

                var knownAggregateIds = string.Join(',', KnownAggregateIds.Select(id => $"'{id}'"));

                await using var dbContext = await dbContextFactory.CreateDbContextAsync();
#pragma warning disable EF1002
                await dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{options.Schema}].[PrimitiveEvent] WHERE Id IN ({knownAggregateIds})");
#pragma warning restore EF1002
            });

        await ExerciseStorageAsync(fixtureConfiguration, isTransactional);
    }


    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_exercise_sequencer_async(bool isTransactional)
    {
        var services = SqlServerFixtureConfiguration.GetServiceCollection();

        await ExercisePrimitiveEventSequencerAsync(new FixtureConfiguration(services)
            .WithAddRecall(builder =>
            {
                builder.Options.EventStore.PrimitiveEventSequencerIdleDurations = [TimeSpan.FromMilliseconds(25)];
            }), isTransactional);
    }
}