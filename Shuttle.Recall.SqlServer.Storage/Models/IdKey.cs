namespace Shuttle.Recall.SqlServer.Storage.Models;

public class IdKey
{
    public string UniqueKey { get; set; } = null!;
    public Guid Id { get; set; }
}