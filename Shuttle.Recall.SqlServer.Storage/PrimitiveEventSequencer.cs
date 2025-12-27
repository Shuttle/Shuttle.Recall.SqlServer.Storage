using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Shuttle.Recall.SqlServer.Storage;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class PrimitiveEventSequencer(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IDbContextFactory<SqlServerStorageDbContext> dbContextFactory) : IPrimitiveEventSequencer
{
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);
    private readonly IDbContextFactory<SqlServerStorageDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);

    public async ValueTask<bool> SequenceAsync(CancellationToken cancellationToken = default)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction =
            await dbContext.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        var rowsAffected = await dbContext.Database.ExecuteSqlRawAsync($@"
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
        SELECT TOP ({_sqlServerStorageOptions.PrimitiveEventSequencerBatchSize})
            [Id],
            [Version],
            ROW_NUMBER() OVER (ORDER BY DateRegistered) AS rn
        FROM 
            [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] WITH (UPDLOCK, ROWLOCK)
        WHERE 
            SequenceNumber IS NULL
        ORDER BY 
            DateRegistered
    )
    UPDATE 
        pe
    SET 
        SequenceNumber = @MaxSequenceNumber + b.rn
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

        return rowsAffected > 0;
    }
}