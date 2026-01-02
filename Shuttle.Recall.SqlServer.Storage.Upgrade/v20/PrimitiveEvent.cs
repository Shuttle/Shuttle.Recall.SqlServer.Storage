namespace Shuttle.Recall.SqlServer.Storage.Upgrade.v20;

public class PrimitiveEvent
{
    public DateTime DateRegistered { get; set; }
    public byte[] EventEnvelope { get; set; } = [];
    public Guid EventId { get; set; }
    public Guid EventTypeId { get; set; } 
    public Guid Id { get; set; }
    public Guid? CorrelationId { get; set; }
    public long SequenceNumber { get; set; }
    public int Version { get; set; }
}