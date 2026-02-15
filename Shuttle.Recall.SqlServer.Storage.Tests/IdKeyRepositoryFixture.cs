using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Recall.Testing;

namespace Shuttle.Recall.SqlServer.Storage.Tests;

public class IdKeyRepositoryFixture
{
    public static readonly Guid Id = new("047FF6FB-FB57-4F63-8795-99F252EDA62F");

    [Test]
    public async Task Should_be_able_to_use_repository_async()
    {
        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<StorageFixture>()
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddLogging()
            .AddRecall(recallBuilder =>
            {
                recallBuilder.UseSqlServerEventStorage(builder =>
                {
                    builder.Options.ConnectionString = configuration.GetConnectionString("Recall") ?? throw new ApplicationException("A 'ConnectionString' with name 'Recall' is required which points to a Sql Server database.");
                    builder.Options.Schema = "recall_fixture";
                });
            });

        var serviceProvider = services.BuildServiceProvider();

        await serviceProvider.StartHostedServicesAsync();

        var repository = serviceProvider.GetRequiredService<IIdKeyRepository>();
        var options = serviceProvider.GetRequiredService<IOptions<SqlServerStorageOptions>>().Value;

        await using var dbContext = serviceProvider.GetRequiredService<SqlServerStorageDbContext>();

#pragma warning disable EF1002
        await dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{options.Schema}].[IdKey] WHERE Id = @Id", new SqlParameter("@Id", Id));
#pragma warning restore EF1002

        var keyA = $"a={Id}";
        var keyB = $"b={Id}";

        await repository.AddAsync(Id, keyA);

        var ex = Assert.ThrowsAsync<SqlException>(async () => await repository.AddAsync(Id, keyA));

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.Message, Does.Contain("Violation of PRIMARY KEY constraint"));

        var id = await repository.FindAsync(keyA);

        Assert.That(id, Is.Not.Null, $"Should be able to retrieve the id of the associated key / id = {Id} / key = '{keyA}'");
        Assert.That(id, Is.EqualTo(Id), $"Should be able to retrieve the correct id of the associated key / id = {Id} / key = '{keyA}' / id retrieved = {id}");

        Assert.That(await repository.FindAsync(keyB), Is.Null, $"Should not be able to get id of non-existent / id = {Id} / key = '{keyB}'");

        await repository.RemoveAsync(Id);

        Assert.That(await repository.FindAsync(keyA), Is.Null, $"Should be able to remove association using id (was not removed) / id = {Id} / key = '{keyA}'");

        await repository.AddAsync(Id, keyA);
        await repository.RemoveAsync(keyA);

        Assert.That(await repository.FindAsync(keyA), Is.Null, $"Should be able to remove association using key (was not removed) / id = {Id} / key = '{keyA}'");

        Assert.That(await repository.ContainsAsync(keyA), Is.False, $"Should not contain key A / key = '{keyA}'");
        Assert.That(await repository.ContainsAsync(keyB), Is.False, $"Should not contain key B / key = '{keyB}'");

        await repository.AddAsync(Id, keyB);

        Assert.That(await repository.ContainsAsync(keyA), Is.False, $"Should not contain key A / key = '{keyA}'");
        Assert.That(await repository.ContainsAsync(keyB), Is.True, $"Should contain key B / key = '{keyB}'");

        await repository.RekeyAsync(keyB, keyA);

        Assert.That(await repository.ContainsAsync(keyA), Is.True, $"Should contain key A / key = '{keyA}'");
        Assert.That(await repository.ContainsAsync(keyB), Is.False, $"Should not contain key B / key = '{keyB}'");

        await repository.AddAsync(Id, keyB);

        Assert.That(await repository.ContainsAsync(keyA), Is.True, $"Should contain key A / key = '{keyA}'");
        Assert.That(await repository.ContainsAsync(keyB), Is.True, $"Should contain key B / key = '{keyB}'");

        ex = Assert.ThrowsAsync<SqlException>(async () => await repository.RekeyAsync(keyA, keyB));

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.Message, Does.Contain("Violation of PRIMARY KEY constraint"));

        await repository.RemoveAsync(Id);

        Assert.That(await repository.ContainsAsync(keyA), Is.False, $"Should not contain key A / key = '{keyA}'");
        Assert.That(await repository.ContainsAsync(keyB), Is.False, $"Should not contain key B / key = '{keyB}'");
    }
}