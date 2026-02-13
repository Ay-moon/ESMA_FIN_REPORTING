:setvar STG_DB "KHLWorldInvest"
:setvar DWH_DB "DWH_KHLWorldInvest"
:setvar AUDIT_DB "AUDIT_BI"

:on error exit

PRINT '=== ESMA FIN REPORTING - SQL Server bootstrap ===';
PRINT 'STG_DB=$(STG_DB) | DWH_DB=$(DWH_DB) | AUDIT_DB=$(AUDIT_DB)';

USE [master];
GO

IF DB_ID('$(AUDIT_DB)') IS NULL
BEGIN
    PRINT 'Creating database $(AUDIT_DB)';
    EXEC ('CREATE DATABASE [$(AUDIT_DB)]');
END
ELSE
    PRINT 'Database $(AUDIT_DB) already exists';
GO

IF DB_ID('$(STG_DB)') IS NULL
BEGIN
    PRINT 'Creating database $(STG_DB)';
    EXEC ('CREATE DATABASE [$(STG_DB)]');
END
ELSE
    PRINT 'Database $(STG_DB) already exists';
GO

IF DB_ID('$(DWH_DB)') IS NULL
BEGIN
    PRINT 'Creating database $(DWH_DB)';
    EXEC ('CREATE DATABASE [$(DWH_DB)]');
END
ELSE
    PRINT 'Database $(DWH_DB) already exists';
GO

/* ------------------------------------------------------------------
   AUDIT_BI
   ------------------------------------------------------------------ */
USE [$(AUDIT_DB)];
GO

IF SCHEMA_ID('log') IS NULL EXEC('CREATE SCHEMA [log]');
GO

:r .\AUDIT_BI_Tables.sql
GO

/* ------------------------------------------------------------------
   STG / CORE DB
   ------------------------------------------------------------------ */
USE [$(STG_DB)];
GO

IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA [stg]');
IF SCHEMA_ID('log') IS NULL EXEC('CREATE SCHEMA [log]');
IF SCHEMA_ID('mart') IS NULL EXEC('CREATE SCHEMA [mart]');
GO

:r .\KHLWorldInvest_Tables.sql
GO
:r .\KHLWorldInvest_Procedures.sql
GO

/* ------------------------------------------------------------------
   DWH / MART DB
   ------------------------------------------------------------------ */
USE [$(DWH_DB)];
GO

IF SCHEMA_ID('mart') IS NULL EXEC('CREATE SCHEMA [mart]');
IF SCHEMA_ID('log') IS NULL EXEC('CREATE SCHEMA [log]');
GO

:r .\DWH_KHLWorldInvest_Tables.sql
GO
:r .\DWH_KHLWorldInvest_Procedures.sql
GO

PRINT '=== Bootstrap completed ===';
