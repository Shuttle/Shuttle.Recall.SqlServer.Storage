namespace Shuttle.Recall.SqlServer.Storage;

public interface IPrimitiveEventQuery
{
    Task<IEnumerable<PrimitiveEvent>> SearchAsync(PrimitiveEvent.Specification specification, CancellationToken cancellationToken = default);
}