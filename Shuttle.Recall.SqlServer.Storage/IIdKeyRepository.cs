namespace Shuttle.Recall.SqlServer.Storage;

public interface IIdKeyRepository
{
    Task AddAsync(Guid id, string key, CancellationToken cancellationToken = default);
    ValueTask<bool> ContainsAsync(string key, CancellationToken cancellationToken = default);
    ValueTask<bool> ContainsAsync(Guid id, CancellationToken cancellationToken = default);
    ValueTask<Guid?> FindAsync(string key, CancellationToken cancellationToken = default);
    Task RekeyAsync(string key, string rekey, CancellationToken cancellationToken = default);
    Task RemoveAsync(string key, CancellationToken cancellationToken = default);
    Task RemoveAsync(Guid id, CancellationToken cancellationToken = default);
}