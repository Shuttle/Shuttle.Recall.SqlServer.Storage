namespace Shuttle.Recall.SqlServer.Storage.Database;

public class UpgradeOptions
{
    public string Connection { get; set; } = string.Empty;
    public string Schema { get; set; } = string.Empty;
    public long FromSequenceNumber { get; set; }
}