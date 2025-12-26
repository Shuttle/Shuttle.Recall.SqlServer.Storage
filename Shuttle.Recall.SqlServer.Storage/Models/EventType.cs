namespace Shuttle.Recall.SqlServer.Storage.Models;

public class EventType
{
    public Guid Id { get; set; }
    public string TypeName { get; set; } = null!;
}