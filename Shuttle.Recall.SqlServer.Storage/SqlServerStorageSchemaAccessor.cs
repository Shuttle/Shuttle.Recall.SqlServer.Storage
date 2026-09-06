using Microsoft.Extensions.Options;
using Shuttle.Contract;

namespace Shuttle.Recall.SqlServer.Storage;

public interface ISqlServerStorageSchemaAccessor
{
    string Schema { get; set; }
}

public class SqlServerStorageSchemaAccessor(IOptions<SqlServerStorageOptions> sqlServerStorageOptions) : ISqlServerStorageSchemaAccessor
{
    public string Schema { get; set; } = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value).Schema;
}
