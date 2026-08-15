# Shuttle.Recall.SqlServer.Storage

Sql Server implementation of the `Shuttle.Recall` event sourcing `IEventStore`.

## Installation

```bash
dotnet add package Shuttle.Recall.SqlServer.Storage
```

## Configuration

In order to use Sql Server for event storage you should use the `UseSqlServerEventStorage` extension:

```c#
services
    .AddRecall()
    .UseSqlServerEventStorage(options =>
    {
        options.ConnectionString = "connection-string";
    });
```

`SqlServerStorageOptions` has the following properties:

| Property | Default | Description |
|----------|---------|-------------|
| `ConnectionString` | `""` | Connection string to the database |
| `Schema` | `"dbo"` | Schema containing the event storage tables |
| `ConfigureDatabase` | `true` | Whether to automatically create/verify the required database structures on startup |
| `CommandTimeout` | `00:00:30` | Command timeout used for the underlying EF Core operations |
| `PrimitiveEventSequencerLimit` | `100` | Batch size used when sequencing newly-saved primitive events |

These can be set directly as above, or bound from configuration yourself, e.g.:

```c#
services.Configure<SqlServerStorageOptions>(configuration.GetSection(SqlServerStorageOptions.SectionName));
```

`SqlServerStorageOptions.SectionName` is `"Shuttle:Recall:SqlServer:Storage"`, so the equivalent `appsettings.json` shape is:

```json
{
  "Shuttle": {
    "Recall": {
      "SqlServer": {
        "Storage": {
          "ConnectionString": "connection-string",
          "Schema": "dbo",
          "ConfigureDatabase": true,
          "CommandTimeout": "00:00:30",
          "PrimitiveEventSequencerLimit": 100
        }
      }
    }
  }
}
```

> This package does not bind that section automatically — the `services.Configure<SqlServerStorageOptions>(...)` call above (or equivalent) is required for the JSON to take effect.

`Shuttle.Recall.SqlServer.EventProcessing` reuses this package's connection (`SqlServerStorageOptions.ConnectionString`/`Schema`/`ConfigureDatabase`), so when both packages are used together, `UseSqlServerEventStorage` must be configured as well as `UseSqlServerEventProcessing`.

## Database

By default, the `SqlServerStorageHostedService` will automatically create the required database structures (`EventType`, `IdKey`, `PrimitiveEvent`) if `ConfigureDatabase` is set to `true` (which is the default). It also detects an outdated schema from an older version of this package and will throw on startup, directing you to upgrade via the console application below.

If you prefer to manage the database structure manually, or need to upgrade an existing database, you can use the provided `Shuttle.Recall.SqlServer.Storage.Database` console application:

```bash
Shuttle.Recall.SqlServer.Storage.Database --connection-string "connection-string" --schema "dbo"
```

| Argument | Alias | Description |
|----------|-------|-------------|
| `--connection-string` | `-cs` | Required. The connection string to the database. |
| `--schema` | `-s` | The schema that contains the `PrimitiveEvent` table. Defaults to `dbo`. |
| `--upgrade` | `-u` | Upgrade an existing (older-version) database instead of configuring a new one. |
| `--from-sequence-number` | `-fsn` | Only relevant with `--upgrade`. Sequence number to start reading from. Defaults to `1`. |

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
