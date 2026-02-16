/*
Centralize ETL logs into AUDIT_BI while keeping legacy writers unchanged.

What this script does:
1) Creates AUDIT_BI.log.finance_load_log (central table) if missing.
2) Creates KHLWorldInvest mapping tables for sync bookkeeping.
3) Creates triggers on KHLWorldInvest log tables:
   - log.ESMA_Load_Log        -> AUDIT_BI.log.ESMA_Load_Log
   - log.finance_load_log     -> AUDIT_BI.log.finance_load_log
4) Backfills existing KHLWorldInvest logs into AUDIT_BI (once).
5) Creates a single consolidated view:
   AUDIT_BI.log.vw_Centralized_Execution_Log
*/

SET NOCOUNT ON;
GO

/* -------------------------------------------------------------------------
   1) AUDIT_BI: central table for finance_load_log
------------------------------------------------------------------------- */
USE [AUDIT_BI];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'log')
BEGIN
    EXEC('CREATE SCHEMA [log]');
END
GO

IF OBJECT_ID(N'[log].[finance_load_log]', N'U') IS NULL
BEGIN
    CREATE TABLE [log].[finance_load_log](
        [load_log_id] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_log_finance_load_log] PRIMARY KEY,
        [source_database] NVARCHAR(128) NOT NULL,
        [source_load_log_id] BIGINT NOT NULL,
        [process_name] NVARCHAR(200) NOT NULL,
        [script_name] NVARCHAR(260) NULL,
        [source_system] NVARCHAR(100) NOT NULL,
        [target_table] NVARCHAR(260) NOT NULL,
        [file_name] NVARCHAR(260) NULL,
        [source_url] NVARCHAR(1000) NULL,
        [produit_type] NVARCHAR(100) NULL,
        [snapshot_ts] DATETIME2(0) NULL,
        [load_start_ts] DATETIME2(0) NOT NULL,
        [load_end_ts] DATETIME2(0) NULL,
        [status] NVARCHAR(30) NOT NULL,
        [rows_read] BIGINT NULL,
        [rows_inserted] BIGINT NULL,
        [rows_rejected] BIGINT NULL,
        [error_message] NVARCHAR(MAX) NULL,
        [checksum] NVARCHAR(200) NULL,
        [params] NVARCHAR(MAX) NULL,
        [extra] NVARCHAR(MAX) NULL,
        CONSTRAINT [UQ_log_finance_load_log_source] UNIQUE ([source_database], [source_load_log_id])
    );
END
GO

/* -------------------------------------------------------------------------
   2) KHLWorldInvest: mapping tables for sync
------------------------------------------------------------------------- */
USE [KHLWorldInvest];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'log')
BEGIN
    EXEC('CREATE SCHEMA [log]');
END
GO

IF OBJECT_ID(N'[log].[ESMA_Load_Log_Audit_Map]', N'U') IS NULL
BEGIN
    CREATE TABLE [log].[ESMA_Load_Log_Audit_Map](
        [SourceLogID] INT NOT NULL CONSTRAINT [PK_ESMA_Load_Log_Audit_Map] PRIMARY KEY,
        [AuditLogID] INT NOT NULL,
        [SyncedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_ESMA_Load_Log_Audit_Map_SyncedAt] DEFAULT (SYSDATETIME())
    );
END
GO

IF OBJECT_ID(N'[log].[finance_load_log_Audit_Map]', N'U') IS NULL
BEGIN
    CREATE TABLE [log].[finance_load_log_Audit_Map](
        [SourceLoadLogID] BIGINT NOT NULL CONSTRAINT [PK_finance_load_log_Audit_Map] PRIMARY KEY,
        [AuditLoadLogID] BIGINT NOT NULL,
        [SyncedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_finance_load_log_Audit_Map_SyncedAt] DEFAULT (SYSDATETIME())
    );
END
GO

/* -------------------------------------------------------------------------
   3) Triggers to mirror logs to AUDIT_BI
------------------------------------------------------------------------- */
CREATE OR ALTER TRIGGER [log].[trg_sync_ESMA_Load_Log_to_AUDIT_BI]
ON [log].[ESMA_Load_Log]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE @new_map TABLE (
            SourceLogID INT NOT NULL,
            AuditLogID INT NOT NULL
        );

        MERGE [AUDIT_BI].[log].[ESMA_Load_Log] AS tgt
        USING
        (
            SELECT
                src.[LogID] AS [SourceLogID],
                src.[ScriptName],
                src.[LaunchTimestamp],
                src.[StartTime],
                src.[EndTime],
                src.[Message],
                src.[FileName],
                src.[Element],
                src.[Complement],
                src.[CreatedOn]
            FROM inserted src
            LEFT JOIN [log].[ESMA_Load_Log_Audit_Map] m
                ON m.[SourceLogID] = src.[LogID]
            WHERE m.[SourceLogID] IS NULL
        ) AS src
        ON 1 = 0
        WHEN NOT MATCHED THEN
            INSERT
            (
                [ScriptName],
                [LaunchTimestamp],
                [StartTime],
                [EndTime],
                [Message],
                [FileName],
                [Element],
                [Complement],
                [CreatedOn]
            )
            VALUES
            (
                src.[ScriptName],
                src.[LaunchTimestamp],
                src.[StartTime],
                src.[EndTime],
                src.[Message],
                src.[FileName],
                src.[Element],
                src.[Complement],
                src.[CreatedOn]
            )
        OUTPUT src.[SourceLogID], inserted.[LogID]
        INTO @new_map(SourceLogID, AuditLogID);

        INSERT INTO [log].[ESMA_Load_Log_Audit_Map]([SourceLogID], [AuditLogID])
        SELECT nm.[SourceLogID], nm.[AuditLogID]
        FROM @new_map nm
        LEFT JOIN [log].[ESMA_Load_Log_Audit_Map] m
            ON m.[SourceLogID] = nm.[SourceLogID]
        WHERE m.[SourceLogID] IS NULL;

        UPDATE tgt
        SET
            tgt.[ScriptName] = src.[ScriptName],
            tgt.[LaunchTimestamp] = src.[LaunchTimestamp],
            tgt.[StartTime] = src.[StartTime],
            tgt.[EndTime] = src.[EndTime],
            tgt.[Message] = src.[Message],
            tgt.[FileName] = src.[FileName],
            tgt.[Element] = src.[Element],
            tgt.[Complement] = src.[Complement],
            tgt.[CreatedOn] = src.[CreatedOn]
        FROM [AUDIT_BI].[log].[ESMA_Load_Log] tgt
        INNER JOIN [log].[ESMA_Load_Log_Audit_Map] m
            ON m.[AuditLogID] = tgt.[LogID]
        INNER JOIN inserted src
            ON src.[LogID] = m.[SourceLogID];
    END TRY
    BEGIN CATCH
        -- Never block ETL in case central sync fails.
        RETURN;
    END CATCH
END
GO

IF OBJECT_ID(N'[log].[finance_load_log]', N'U') IS NOT NULL
BEGIN
    EXEC('
    CREATE OR ALTER TRIGGER [log].[trg_sync_finance_load_log_to_AUDIT_BI]
    ON [log].[finance_load_log]
    AFTER INSERT, UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;

        BEGIN TRY
            DECLARE @new_map TABLE (
                SourceLoadLogID BIGINT NOT NULL,
                AuditLoadLogID BIGINT NOT NULL
            );

            MERGE [AUDIT_BI].[log].[finance_load_log] AS tgt
            USING
            (
                SELECT
                    src.[load_log_id] AS [SourceLoadLogID],
                    DB_NAME() AS [source_database],
                    src.[process_name],
                    src.[script_name],
                    src.[source_system],
                    src.[target_table],
                    src.[file_name],
                    src.[source_url],
                    src.[produit_type],
                    src.[snapshot_ts],
                    src.[load_start_ts],
                    src.[load_end_ts],
                    src.[status],
                    src.[rows_read],
                    src.[rows_inserted],
                    src.[rows_rejected],
                    src.[error_message],
                    src.[checksum],
                    src.[params],
                    src.[extra]
                FROM inserted src
                LEFT JOIN [log].[finance_load_log_Audit_Map] m
                    ON m.[SourceLoadLogID] = src.[load_log_id]
                WHERE m.[SourceLoadLogID] IS NULL
            ) AS src
            ON 1 = 0
            WHEN NOT MATCHED THEN
                INSERT
                (
                    [source_database],
                    [source_load_log_id],
                    [process_name],
                    [script_name],
                    [source_system],
                    [target_table],
                    [file_name],
                    [source_url],
                    [produit_type],
                    [snapshot_ts],
                    [load_start_ts],
                    [load_end_ts],
                    [status],
                    [rows_read],
                    [rows_inserted],
                    [rows_rejected],
                    [error_message],
                    [checksum],
                    [params],
                    [extra]
                )
                VALUES
                (
                    src.[source_database],
                    src.[SourceLoadLogID],
                    src.[process_name],
                    src.[script_name],
                    src.[source_system],
                    src.[target_table],
                    src.[file_name],
                    src.[source_url],
                    src.[produit_type],
                    src.[snapshot_ts],
                    src.[load_start_ts],
                    src.[load_end_ts],
                    src.[status],
                    src.[rows_read],
                    src.[rows_inserted],
                    src.[rows_rejected],
                    src.[error_message],
                    src.[checksum],
                    src.[params],
                    src.[extra]
                )
            OUTPUT src.[SourceLoadLogID], inserted.[load_log_id]
            INTO @new_map(SourceLoadLogID, AuditLoadLogID);

            INSERT INTO [log].[finance_load_log_Audit_Map]([SourceLoadLogID], [AuditLoadLogID])
            SELECT nm.[SourceLoadLogID], nm.[AuditLoadLogID]
            FROM @new_map nm
            LEFT JOIN [log].[finance_load_log_Audit_Map] m
                ON m.[SourceLoadLogID] = nm.[SourceLoadLogID]
            WHERE m.[SourceLoadLogID] IS NULL;

            UPDATE tgt
            SET
                tgt.[process_name] = src.[process_name],
                tgt.[script_name] = src.[script_name],
                tgt.[source_system] = src.[source_system],
                tgt.[target_table] = src.[target_table],
                tgt.[file_name] = src.[file_name],
                tgt.[source_url] = src.[source_url],
                tgt.[produit_type] = src.[produit_type],
                tgt.[snapshot_ts] = src.[snapshot_ts],
                tgt.[load_start_ts] = src.[load_start_ts],
                tgt.[load_end_ts] = src.[load_end_ts],
                tgt.[status] = src.[status],
                tgt.[rows_read] = src.[rows_read],
                tgt.[rows_inserted] = src.[rows_inserted],
                tgt.[rows_rejected] = src.[rows_rejected],
                tgt.[error_message] = src.[error_message],
                tgt.[checksum] = src.[checksum],
                tgt.[params] = src.[params],
                tgt.[extra] = src.[extra]
            FROM [AUDIT_BI].[log].[finance_load_log] tgt
            INNER JOIN [log].[finance_load_log_Audit_Map] m
                ON m.[AuditLoadLogID] = tgt.[load_log_id]
            INNER JOIN inserted src
                ON src.[load_log_id] = m.[SourceLoadLogID];
        END TRY
        BEGIN CATCH
            -- Never block ETL in case central sync fails.
            RETURN;
        END CATCH
    END
    ');
END
GO

/* -------------------------------------------------------------------------
   4) One-shot backfill existing KHL logs into AUDIT_BI
------------------------------------------------------------------------- */
BEGIN TRY
    DECLARE @esma_backfill_map TABLE (
        SourceLogID INT NOT NULL,
        AuditLogID INT NOT NULL
    );

    MERGE [AUDIT_BI].[log].[ESMA_Load_Log] AS tgt
    USING
    (
        SELECT
            src.[LogID] AS [SourceLogID],
            src.[ScriptName],
            src.[LaunchTimestamp],
            src.[StartTime],
            src.[EndTime],
            src.[Message],
            src.[FileName],
            src.[Element],
            src.[Complement],
            src.[CreatedOn]
        FROM [log].[ESMA_Load_Log] src
        LEFT JOIN [log].[ESMA_Load_Log_Audit_Map] m
            ON m.[SourceLogID] = src.[LogID]
        WHERE m.[SourceLogID] IS NULL
    ) AS src
    ON 1 = 0
    WHEN NOT MATCHED THEN
        INSERT
        (
            [ScriptName],
            [LaunchTimestamp],
            [StartTime],
            [EndTime],
            [Message],
            [FileName],
            [Element],
            [Complement],
            [CreatedOn]
        )
        VALUES
        (
            src.[ScriptName],
            src.[LaunchTimestamp],
            src.[StartTime],
            src.[EndTime],
            src.[Message],
            src.[FileName],
            src.[Element],
            src.[Complement],
            src.[CreatedOn]
        )
    OUTPUT src.[SourceLogID], inserted.[LogID]
    INTO @esma_backfill_map(SourceLogID, AuditLogID);

    INSERT INTO [log].[ESMA_Load_Log_Audit_Map]([SourceLogID], [AuditLogID])
    SELECT nm.[SourceLogID], nm.[AuditLogID]
    FROM @esma_backfill_map nm
    LEFT JOIN [log].[ESMA_Load_Log_Audit_Map] m
        ON m.[SourceLogID] = nm.[SourceLogID]
    WHERE m.[SourceLogID] IS NULL;
END TRY
BEGIN CATCH
    -- keep script idempotent/non-blocking
END CATCH;
GO

IF OBJECT_ID(N'[log].[finance_load_log]', N'U') IS NOT NULL
BEGIN
    BEGIN TRY
        DECLARE @fin_backfill_map TABLE (
            SourceLoadLogID BIGINT NOT NULL,
            AuditLoadLogID BIGINT NOT NULL
        );

        MERGE [AUDIT_BI].[log].[finance_load_log] AS tgt
        USING
        (
            SELECT
                src.[load_log_id] AS [SourceLoadLogID],
                DB_NAME() AS [source_database],
                src.[process_name],
                src.[script_name],
                src.[source_system],
                src.[target_table],
                src.[file_name],
                src.[source_url],
                src.[produit_type],
                src.[snapshot_ts],
                src.[load_start_ts],
                src.[load_end_ts],
                src.[status],
                src.[rows_read],
                src.[rows_inserted],
                src.[rows_rejected],
                src.[error_message],
                src.[checksum],
                src.[params],
                src.[extra]
            FROM [log].[finance_load_log] src
            LEFT JOIN [log].[finance_load_log_Audit_Map] m
                ON m.[SourceLoadLogID] = src.[load_log_id]
            WHERE m.[SourceLoadLogID] IS NULL
        ) AS src
        ON 1 = 0
        WHEN NOT MATCHED THEN
            INSERT
            (
                [source_database],
                [source_load_log_id],
                [process_name],
                [script_name],
                [source_system],
                [target_table],
                [file_name],
                [source_url],
                [produit_type],
                [snapshot_ts],
                [load_start_ts],
                [load_end_ts],
                [status],
                [rows_read],
                [rows_inserted],
                [rows_rejected],
                [error_message],
                [checksum],
                [params],
                [extra]
            )
            VALUES
            (
                src.[source_database],
                src.[SourceLoadLogID],
                src.[process_name],
                src.[script_name],
                src.[source_system],
                src.[target_table],
                src.[file_name],
                src.[source_url],
                src.[produit_type],
                src.[snapshot_ts],
                src.[load_start_ts],
                src.[load_end_ts],
                src.[status],
                src.[rows_read],
                src.[rows_inserted],
                src.[rows_rejected],
                src.[error_message],
                src.[checksum],
                src.[params],
                src.[extra]
            )
        OUTPUT src.[SourceLoadLogID], inserted.[load_log_id]
        INTO @fin_backfill_map(SourceLoadLogID, AuditLoadLogID);

        INSERT INTO [log].[finance_load_log_Audit_Map]([SourceLoadLogID], [AuditLoadLogID])
        SELECT nm.[SourceLoadLogID], nm.[AuditLoadLogID]
        FROM @fin_backfill_map nm
        LEFT JOIN [log].[finance_load_log_Audit_Map] m
            ON m.[SourceLoadLogID] = nm.[SourceLoadLogID]
        WHERE m.[SourceLoadLogID] IS NULL;
    END TRY
    BEGIN CATCH
        -- keep script idempotent/non-blocking
    END CATCH;
END
GO

/* -------------------------------------------------------------------------
   5) AUDIT_BI: single consolidated view for operations
------------------------------------------------------------------------- */
USE [AUDIT_BI];
GO

CREATE OR ALTER VIEW [log].[vw_Centralized_Execution_Log]
AS
SELECT
    CAST(N'ESMA_Load_Log' AS NVARCHAR(64)) AS [LogSource],
    CAST([LaunchTimestamp] AS DATETIME2(0)) AS [EventTime],
    [ScriptName] AS [ProcessName],
    [Element] AS [Element],
    [Message] AS [Message],
    [Complement] AS [Details],
    CAST(NULL AS NVARCHAR(100)) AS [Status],
    CAST(NULL AS BIGINT) AS [RowsRead],
    CAST(NULL AS BIGINT) AS [RowsInserted],
    CAST(NULL AS BIGINT) AS [RowsRejected]
FROM [log].[ESMA_Load_Log]

UNION ALL

SELECT
    CAST(N'finance_load_log' AS NVARCHAR(64)) AS [LogSource],
    CAST([load_start_ts] AS DATETIME2(0)) AS [EventTime],
    [process_name] AS [ProcessName],
    [produit_type] AS [Element],
    CONCAT(N'file=', COALESCE([file_name], N''), N' target=', COALESCE([target_table], N'')) AS [Message],
    [error_message] AS [Details],
    [status] AS [Status],
    [rows_read] AS [RowsRead],
    [rows_inserted] AS [RowsInserted],
    [rows_rejected] AS [RowsRejected]
FROM [log].[finance_load_log]

UNION ALL

SELECT
    CAST(N'AuditLog_SystemHealth' AS NVARCHAR(64)) AS [LogSource],
    CAST([CheckDate] AS DATETIME2(0)) AS [EventTime],
    CAST(N'SystemHealth' AS NVARCHAR(255)) AS [ProcessName],
    [Status] AS [Element],
    CONCAT(N'DB=', COALESCE([SourceDatabase], N''), N' sizeMB=', COALESCE(CONVERT(NVARCHAR(64), [DatabaseSize_MB]), N'')) AS [Message],
    [Details] AS [Details],
    [Status] AS [Status],
    CAST(NULL AS BIGINT) AS [RowsRead],
    CAST(NULL AS BIGINT) AS [RowsInserted],
    CAST(NULL AS BIGINT) AS [RowsRejected]
FROM [dbo].[AuditLog_SystemHealth];
GO
