# Shuttle.Recall.SqlServer.Storage

```
PM> Install-Package Shuttle.Recall.SqlServer.Storage
```

A Sql Server implementation of the `Shuttle.Recall` event sourcing `EventStore`.

## Configuration

```c#
services.AddSqlEventStorage();
```

## Database

Please reference the `Shuttle.Recall.EntityFrameworkCore.SqlServer.Storage` package and use the Entity Framework Core migration mechanism to create the event store database structures.

## IKeyStore

You are bound to run into situations where you have a business or other key that is required to be unique.  Given that the `IEventStore` makes use of only surrogate keys the `IKeyStore` is used to create a unique list of keys associated with a given aggregate identifier.

Since the keys used in the key store have to be unique you should ensure that they contain enough information to be unique and have the intended meaning.

A key could be something such as `[order-number]:ord-001/2016`, `[customer-onboarding]:id-number=0000005555089`, or `[system-name/profile]:672cda1c-c3ec-4f81-a577-e64f9f14e141`.

### Contains

``` c#
bool Contains(string key);
```

Returns `true` if the given `key` has an associated aggregate identifier.

---
``` c#
bool Contains(Guid id);
```

Returns `true` if the given `id` is present in the key store.

---
### Get

``` c#
Guid? Get(string key);
```

Returns the `Guid` associated with the given key; else `null`.

---
### Remove

``` c#
void Remove(string key);
void Remove(Guid id);
```

When specifying the `key` the assocation with the identifier will be removed.  When specifying the `id` all keys associated with the given `id` will be removed.

---
### Add

``` c#
void Add(Guid id, string key);
```

Creates an association between the `id` and the `key`.

---
### Add

``` c#
void Rekey(string key, string rekey);
```

Changes `key` to a new key specified by `rekey`.

