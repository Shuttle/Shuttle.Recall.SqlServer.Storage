using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.Storage;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class PrimitiveEventSequencer(IOptions<RecallOptions> recallOptions, IOptions<SqlServerStorageOptions> sqlServerStorageOptions, SqlServerStorageDbContext dbContext) : IPrimitiveEventSequencer
{
    private readonly RecallOptions _recallOptions = Guard.AgainstNull(Guard.AgainstNull(recallOptions).Value);
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);
    private readonly SqlServerStorageDbContext _dbContext = Guard.AgainstNull(dbContext);

    public async ValueTask<bool> SequenceAsync(CancellationToken cancellationToken = default)
    {
        await _recallOptions.Operation.InvokeAsync(new("[PrimitiveEventSequencer/Starting]"), cancellationToken);

        await using var transaction = await _dbContext.Database.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

        var rowsAffected = await _dbContext.Database.ExecuteSqlRawAsync($@"
DECLARE @lock_result INT;
DECLARE @MaxSequenceNumber BIGINT;
DECLARE @RowsAffected INT = 0;

EXEC @lock_result = sp_getapplock @Resource = '{typeof(PrimitiveEventSequencer).FullName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

IF @lock_result < 0
    THROW 50000, 'Failed to acquire schema lock', 1;

BEGIN TRY
    SELECT 
        @MaxSequenceNumber = ISNULL(MAX(SequenceNumber), 0)
    FROM 
        [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent];

    ;WITH Batch AS
    (
        SELECT TOP ({_sqlServerStorageOptions.PrimitiveEventSequencerLimit})
            [Id],
            [Version],
            ROW_NUMBER() OVER (ORDER BY [RecordedAt], [Version]) AS rn
        FROM 
            [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] WITH (UPDLOCK, ROWLOCK)
        WHERE 
            [SequenceNumber] IS NULL
        ORDER BY 
            [RecordedAt], [Version]
    )
    UPDATE 
        pe
    SET 
        [SequenceNumber] = @MaxSequenceNumber + b.rn
    FROM 
        [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] pe
    INNER JOIN 
        Batch b ON b.Id = pe.Id AND b.Version = pe.Version;

    SET @RowsAffected = @@ROWCOUNT;
END TRY
BEGIN CATCH
    EXEC sp_releaseapplock @Resource = '{typeof(PrimitiveEventSequencer).FullName}', @LockOwner = 'Session';
    THROW;
END CATCH

EXEC sp_releaseapplock @Resource = '{typeof(PrimitiveEventSequencer).FullName}', @LockOwner = 'Session';

SELECT @RowsAffected;
", cancellationToken);

        await transaction.CommitAsync(cancellationToken);

        await _recallOptions.Operation.InvokeAsync(new($"[PrimitiveEventSequencer/Completed] : rows affected = {rowsAffected}"), cancellationToken);

        return rowsAffected > 0;
    }
}