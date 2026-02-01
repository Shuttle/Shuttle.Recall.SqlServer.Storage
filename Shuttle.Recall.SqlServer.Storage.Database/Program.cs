using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Events;
using Shuttle.Core.Cli;

namespace Shuttle.Recall.SqlServer.Storage.Database;

internal class Program
{
    private static async Task Main()
    {
        var args = Arguments.FromCommandLine()
            .Add(new ArgumentDefinition("connection-string", "cs").WithDescription("The connection string to the database.").AsRequired())
            .Add(new ArgumentDefinition("schema", "s").WithDescription("The schema that contains the currentPrimitiveEvent table."))
            .Add(new ArgumentDefinition("from-sequence-number", "fsn").WithDescription("Sequence number to start reading from.  Defaults to 1."))
            .Add(new ArgumentDefinition("help", "h", "?"));

        if (args.Contains("help"))
        {
            Console.WriteLine();
            Console.WriteLine(@$"Usage: {Path.GetFileName(Environment.ProcessPath)} [options]");
            Console.WriteLine();
            Console.WriteLine(@"Options:");
            Console.WriteLine(args.GetDefinitionText(Console.WindowWidth));

            return;
        }

        if (args.HasMissingValues())
        {
            Console.WriteLine();
            Console.WriteLine(@$"Usage: {Path.GetFileName(Environment.ProcessPath)} [options]");
            Console.WriteLine();
            Console.WriteLine(@"Options (required):");
            Console.WriteLine(args.GetDefinitionText(Console.WindowWidth, true));

            return;
        }

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
            .MinimumLevel.Override("Shuttle", LogEventLevel.Verbose)
            .WriteTo.Console()
            .WriteTo.File(
                "./logs/.log",
                rollingInterval: RollingInterval.Day,
                rollOnFileSizeLimit: true,
                fileSizeLimitBytes: 1_048_576, // 1 MB
                retainedFileCountLimit: 30
            )
            .CreateLogger();

        var services = new ServiceCollection()
            .AddOptions()
            .Configure<RecallOptions>(options =>
            {
                options.Operation += (eventArgs, _) =>
                {
                    Log.Information(eventArgs.Operation);
                    return Task.CompletedTask;
                };
            })
            .Configure<SqlServerStorageOptions>(options =>
            {
                options.ConnectionString = args.Get<string>("connection-string");
                options.Schema = args.Get<string>("schema", "dbo");
                options.ConfigureDatabase = true;
            })
            .AddDbContext<SqlServerStorageDbContext>(builder => builder.UseSqlServer(args.Get<string>("connection-string")))
            .AddDbContext<SqlServerStorageDbContext>(builder => builder.UseSqlServer(args.Get<string>("connection-string")))
            .AddSingleton<SqlServerStorageHostedService>();

        var serviceProvider = services.BuildServiceProvider();

        var hostedService = serviceProvider.GetRequiredService<SqlServerStorageHostedService>();

        await hostedService.StartAsync(CancellationToken.None);

        await new UpgradePrimitiveEventService(Log.ForContext<UpgradePrimitiveEventService>(), args.Get<string>("connection-string"), args.Get<string>("schema", "dbo"), args.Get<long>("from-sequence-number", 1))
            .ExecuteAsync();
    }
}