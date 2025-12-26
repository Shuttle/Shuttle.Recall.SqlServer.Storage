namespace Shuttle.Recall.SqlServer.Storage;

public interface IEventTypeRepository
{
    Task<Guid> GetIdAsync(string typeName, CancellationToken cancellationToken = default);
}