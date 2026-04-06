namespace Shuttle.Recall.SqlServer.Storage;

public class SqlServerStorageOptions
{
    public const string SectionName = "Shuttle:Recall:SqlServer:Storage";

    public string ConnectionString { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public bool ConfigureDatabase { get; set; } = true;
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int PrimitiveEventSequencerLimit { get; set; } = 100;
}