using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.Storage;

public class SqlServerStorageOptionsValidator : IValidateOptions<SqlServerStorageOptions>
{
    public ValidateOptionsResult Validate(string? name, SqlServerStorageOptions options)
    {
        Guard.AgainstNull(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail(Resources.ConnectionStringOptionException);
        }

        if (string.IsNullOrWhiteSpace(options.Schema))
        {
            return ValidateOptionsResult.Fail(Resources.SchemaOptionException);
        }

        return ValidateOptionsResult.Success;
    }
}