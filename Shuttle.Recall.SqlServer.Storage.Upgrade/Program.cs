using Serilog;
using Serilog.Events;
using Shuttle.Core.Cli;

namespace Shuttle.Recall.SqlServer.Storage.Upgrade;

internal class Program
{
    static async Task Main()
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
                path: "./logs/.log",
                rollingInterval: RollingInterval.Day,
                rollOnFileSizeLimit: true,
                fileSizeLimitBytes: 1_048_576, // 1 MB
                retainedFileCountLimit: 30
            )
            .CreateLogger();
        
        await new UpgradePrimitiveEventService(Log.ForContext<UpgradePrimitiveEventService>(), args.Get<string>("connection-string"), args.Get<string>("schema", "dbo"), args.Get<long>("from-sequence-number", 1))
            .ExecuteAsync();
    }
}
