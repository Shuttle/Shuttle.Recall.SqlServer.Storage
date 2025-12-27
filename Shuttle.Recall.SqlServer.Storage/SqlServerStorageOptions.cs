namespace Shuttle.Recall.SqlServer.Storage;

public class SqlServerStorageOptions
{
    public const string SectionName = "Shuttle:EventStore:SqlServer:Storage";

    public string ConnectionString { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public bool ConfigureDatabase { get; set; } = true;
    public int CommandTimeout { get; set; } = 30;
    public int PrimitiveEventSequencerBatchSize { get; set; } = 100;
}