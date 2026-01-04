using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using System.Diagnostics.CodeAnalysis;
using Shuttle.Core.Reflection;

namespace Shuttle.Recall.SqlServer.Storage;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class SqlServerStorageHostedService(IOptions<SqlServerStorageOptions> sqlServerStorageOptions, IDbContextFactory<SqlServerStorageDbContext> dbContextFactory) : IHostedService
{
    private readonly SqlServerStorageOptions _sqlServerStorageOptions = Guard.AgainstNull(Guard.AgainstNull(sqlServerStorageOptions).Value);
    private readonly IDbContextFactory<SqlServerStorageDbContext> _dbContextFactory = Guard.AgainstNull(dbContextFactory);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_sqlServerStorageOptions.ConfigureDatabase)
        {
            return;
        }

        var retry = true;
        var retryCount = 0;

        while (retry)
        {
            try
            {
                await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

                await dbContext.Database.ExecuteSqlRawAsync($@"
DECLARE @lock_result INT;
EXEC @lock_result = sp_getapplock @Resource = '{typeof(SqlServerStorageHostedService).FullName}', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 15000;

IF @lock_result < 0
    THROW 50000, 'Failed to acquire schema lock', 1;

BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_sqlServerStorageOptions.Schema}')
    BEGIN
        EXEC('CREATE SCHEMA [{_sqlServerStorageOptions.Schema}]');
    END

    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[EventType]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [{_sqlServerStorageOptions.Schema}].[EventType]
        (
            [Id] [uniqueidentifier] NOT NULL,
            [TypeName] [nvarchar](1024) NOT NULL,
            CONSTRAINT [PK_EventType] PRIMARY KEY CLUSTERED ([Id] ASC)
        ) ON [PRIMARY];

        CREATE UNIQUE NONCLUSTERED INDEX [IX_EventType] ON [{_sqlServerStorageOptions.Schema}].[EventType] ([TypeName] ASC);
    END

    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[IdKey]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [{_sqlServerStorageOptions.Schema}].[IdKey]
        (
            [UniqueKey] [nvarchar](450) NOT NULL,
            [Id] [uniqueidentifier] NOT NULL,
            CONSTRAINT [PK_IdKey] PRIMARY KEY CLUSTERED ([UniqueKey] ASC)
        ) ON [PRIMARY];
    END

    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]
        (
            [Id] [uniqueidentifier] NOT NULL,
            [Version] [int] NOT NULL,
            [CorrelationId] [uniqueidentifier] NULL,
            [EventEnvelope] [varbinary](max) NOT NULL,
            [EventId] [uniqueidentifier] NOT NULL,
            [EventTypeId] [uniqueidentifier] NOT NULL,
            [RecordedAt] [datetimeoffset](7) NOT NULL,
            [SequenceNumber] [bigint] NULL,
            CONSTRAINT [PK_PrimitiveEvent] PRIMARY KEY CLUSTERED ([Id] ASC, [Version] ASC)
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];

        ALTER TABLE [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]
            ADD CONSTRAINT [DF_PrimitiveEvent_RecordedAt] DEFAULT (SYSUTCDATETIME()) FOR [RecordedAt];

        ALTER TABLE [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] 
            WITH CHECK ADD CONSTRAINT [FK_PrimitiveEvent_EventType_EventTypeId] 
            FOREIGN KEY([EventTypeId]) REFERENCES [{_sqlServerStorageOptions.Schema}].[EventType] ([Id]) ON DELETE CASCADE;

        ALTER TABLE [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] 
            CHECK CONSTRAINT [FK_PrimitiveEvent_EventType_EventTypeId];

        CREATE NONCLUSTERED INDEX [IX_PrimitiveEvent_EventTypeId] 
            ON [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] ([EventTypeId] ASC);

        CREATE UNIQUE NONCLUSTERED INDEX [IX_PrimitiveEvent_SequenceNumber] 
            ON [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] ([SequenceNumber] ASC, [RecordedAt] ASC)
            WHERE [SequenceNumber] IS NOT NULL;

        CREATE NONCLUSTERED INDEX [IX_PrimitiveEvent_NullSequence_RecordedAt_Version] 
            ON [{_sqlServerStorageOptions.Schema}].[PrimitiveEvent] ([RecordedAt] ASC, [Version] ASC)
            WHERE [SequenceNumber] IS NULL;
    END
    ELSE
    BEGIN
        DECLARE @needsUpgrade BIT = 0;

        IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]') AND name = 'SequenceNumber')
        BEGIN
            IF COLUMNPROPERTY(OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]'), 'SequenceNumber', 'IsIdentity') = 1
            BEGIN
                SET @needsUpgrade = 1;
            END
        END

        IF EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{_sqlServerStorageOptions.Schema}'
              AND TABLE_NAME = 'PrimitiveEvent'
              AND COLUMN_NAME = 'SequenceNumber'
              AND IS_NULLABLE = 'NO'
        )
        BEGIN
            SET @needsUpgrade = 1;
        END

        IF EXISTS (
            SELECT 1
            FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]')
              AND name = 'DateRegistered'
        )
        BEGIN
            SET @needsUpgrade = 1;
        END

        IF EXISTS (
            SELECT * FROM sys.columns 
            WHERE object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]') 
            AND name = 'DateCommitted'
        )
        BEGIN
            SET @needsUpgrade = 1;
        END

        IF EXISTS (
            SELECT * FROM sys.indexes 
            WHERE name = N'IX_PrimitiveEvent_DateCommitted_Filtered_Null' 
            AND object_id = OBJECT_ID(N'[{_sqlServerStorageOptions.Schema}].[PrimitiveEvent]')
        )
        BEGIN
            SET @needsUpgrade = 1;
        END

        IF EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{_sqlServerStorageOptions.Schema}'
              AND TABLE_NAME = 'PrimitiveEvent'
              AND COLUMN_NAME = 'RecordedAt'
              AND DATA_TYPE = 'datetime2'
        )
        BEGIN
            SET @needsUpgrade = 1;
        END

        IF @needsUpgrade = 1
        BEGIN
            THROW 50001, 'Database schema is outdated. Please upgrade the database to version 21.0.0 using the Shuttle.Recall.SqlServer.Storage.Upgrade application.', 1;
        END
    END

END TRY
BEGIN CATCH
    EXEC sp_releaseapplock @Resource = '{typeof(SqlServerStorageHostedService).FullName}', @LockOwner = 'Session';
    THROW;
END CATCH

EXEC sp_releaseapplock @Resource = '{typeof(SqlServerStorageHostedService).FullName}', @LockOwner = 'Session';
", cancellationToken: cancellationToken);

                retry = false;
            }
            catch(Exception ex)
            {
                retryCount++;

                if (retryCount > 3 || ex.AllMessages().Contains("Database schema is outdated"))
                {
                    throw;
                }
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}