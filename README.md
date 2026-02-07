# Shuttle.Recall.SqlServer.Storage

Sql Server implementation of the `Shuttle.Recall` event sourcing `IEventStore`.

## Installation

```bash
dotnet add package Shuttle.Recall.SqlServer.Storage
```

## Configuration

In order to use Sql Server for event storage you should use the `UseSqlServerEventStorage` extension:

```c#
services.AddRecall(builder =>
{
    builder.UseSqlServerEventStorage(options =>
    {
        options.ConnectionString = "connection-string";
    });
});
```

The options can also be configured via `appsettings.json`:

```json
{
  "Shuttle": {
    "Recall": {
      "SqlServer": {
        "Storage": {
          "ConnectionString": "connection-string",
          "Schema": "dbo"
        }
      }
    }
  }
}
```

## Database

By default, the `SqlServerStorageHostedService` will automatically create the required database structures if `ConfigureDatabase` is set to `true` (which is the default). If you prefer to manage the database structure manually, you can use the provided `Shuttle.Recall.SqlServer.Storage.Database` console application.

## IIdKeyRepository

You are bound to run into situations where you have a business or other key that is required to be unique. Given that the `IEventStore` makes use of only surrogate keys, the `IIdKeyRepository` is used to create a unique list of keys associated with a given aggregate identifier.

Since the keys used in the key store have to be unique, you should ensure that they contain enough information to be unique and have the intended meaning.

A key could be something such as `[order-number]:ord-001/2016`, `[customer-onboarding]:id-number=0000005555089`, or `[system-name/profile]:672cda1c-c3ec-4f81-a577-e64f9f14e141`.

### ContainsAsync

```c#
ValueTask<bool> ContainsAsync(string key, CancellationToken cancellationToken = default);
ValueTask<bool> ContainsAsync(Guid id, CancellationToken cancellationToken = default);
```

Returns `true` if the given `key` or `id` is present in the key store.

### FindAsync

```c#
ValueTask<Guid?> FindAsync(string key, CancellationToken cancellationToken = default);
```

Returns the `Guid` associated with the given key; else `null`.

### RemoveAsync

```c#
Task RemoveAsync(string key, CancellationToken cancellationToken = default);
Task RemoveAsync(Guid id, CancellationToken cancellationToken = default);
```

When specifying the `key`, the association with the identifier will be removed. When specifying the `id`, all keys associated with the given `id` will be removed.

### AddAsync

```c#
Task AddAsync(Guid id, string key, CancellationToken cancellationToken = default);
```

Creates an association between the `id` and the `key`.

### RekeyAsync

```c#
Task RekeyAsync(string key, string rekey, CancellationToken cancellationToken = default);
```

Changes `key` to a new key specified by `rekey`.

