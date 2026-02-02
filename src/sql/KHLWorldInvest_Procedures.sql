SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF

/* -----------------------------------------------------------------------------
   Logger: dynamic column mapping to [AUDIT_BI].[log].[ESMA_Load_Log] (robust + simple).
   ----------------------------------------------------------------------------- */
CREATE PROCEDURE log.usp_ESMA_WriteLog
    @ProcessName nvarchar(200),
    @StepName    nvarchar(200) = NULL,
    @LogLevel    nvarchar(20)  = N'INFO',
    @Message     nvarchar(4000),
    @EventUTC    datetime2(0)  = NULL,
    @RowCount    int           = NULL,
    @DetailsJson nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @EventUTC = COALESCE(@EventUTC, SYSUTCDATETIME());

    IF OBJECT_ID('[AUDIT_BI].[log].[ESMA_Load_Log]','U') IS NULL RETURN;

    DECLARE @cols nvarchar(max) = N'';
    DECLARE @vals nvarchar(max) = N'';

    DECLARE @procCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','ProcessName') IS NOT NULL SET @procCol = 'ProcessName';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','Process') IS NOT NULL SET @procCol = 'Process';

    DECLARE @stepCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','StepName') IS NOT NULL SET @stepCol = 'StepName';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','Step') IS NOT NULL SET @stepCol = 'Step';

    DECLARE @lvlCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','LogLevel') IS NOT NULL SET @lvlCol = 'LogLevel';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','Level') IS NOT NULL SET @lvlCol = 'Level';

    DECLARE @msgCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','Message') IS NOT NULL SET @msgCol = 'Message';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','LogMessage') IS NOT NULL SET @msgCol = 'LogMessage';

    DECLARE @evtCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','EventUTC') IS NOT NULL SET @evtCol = 'EventUTC';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','EventDtmUTC') IS NOT NULL SET @evtCol = 'EventDtmUTC';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','LoadDtmUTC') IS NOT NULL SET @evtCol = 'LoadDtmUTC';

    DECLARE @rowCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','RowCount') IS NOT NULL SET @rowCol = 'RowCount';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','Rows') IS NOT NULL SET @rowCol = 'Rows';

    DECLARE @detCol sysname = NULL;
    IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','DetailsJson') IS NOT NULL SET @detCol = 'DetailsJson';
    ELSE IF COL_LENGTH('[AUDIT_BI].[log].[ESMA_Load_Log]','Details') IS NOT NULL SET @detCol = 'Details';

    IF @procCol IS NOT NULL BEGIN SET @cols += QUOTENAME(@procCol)+N','; SET @vals += N'@ProcessName,'; END;
    IF @stepCol IS NOT NULL BEGIN SET @cols += QUOTENAME(@stepCol)+N','; SET @vals += N'@StepName,'; END;
    IF @lvlCol  IS NOT NULL BEGIN SET @cols += QUOTENAME(@lvlCol)+N',';  SET @vals += N'@LogLevel,'; END;
    IF @msgCol  IS NOT NULL BEGIN SET @cols += QUOTENAME(@msgCol)+N',';  SET @vals += N'@Message,'; END;
    IF @evtCol  IS NOT NULL BEGIN SET @cols += QUOTENAME(@evtCol)+N',';  SET @vals += N'@EventUTC,'; END;
    IF @rowCol  IS NOT NULL BEGIN SET @cols += QUOTENAME(@rowCol)+N',';  SET @vals += N'@RowCount,'; END;
    IF @detCol  IS NOT NULL BEGIN SET @cols += QUOTENAME(@detCol)+N',';  SET @vals += N'@DetailsJson,'; END;

    IF @cols = N'' RETURN;

    SET @cols = LEFT(@cols, LEN(@cols)-1);
    SET @vals = LEFT(@vals, LEN(@vals)-1);

    DECLARE @sql nvarchar(max) = N'INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] (' + @cols + N') VALUES (' + @vals + N');';

    EXEC sp_executesql
        @sql,
        N'@ProcessName nvarchar(200),@StepName nvarchar(200),@LogLevel nvarchar(20),
          @Message nvarchar(4000),@EventUTC datetime2(0),@RowCount int,@DetailsJson nvarchar(max)',
        @ProcessName=@ProcessName,@StepName=@StepName,@LogLevel=@LogLevel,
        @Message=@Message,@EventUTC=@EventUTC,@RowCount=@RowCount,@DetailsJson=@DetailsJson;
END

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF

CREATE PROCEDURE [stg].[usp_Load_ESMA_INSTRUMENTS_From_FULINS_WIDE]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ScriptName nvarchar(255) = N'stg.usp_Load_ESMA_INSTRUMENTS_From_FULINS_WIDE';
    DECLARE @LaunchTs  datetime2(0)   = SYSDATETIME();

    DECLARE @LastFullValidFromDate date;

    DECLARE
        @Before_ListCnt  bigint,
        @Before_DebtCnt  bigint,
        @Before_DerivCnt bigint,
        @After_ListCnt   bigint,
        @After_DebtCnt   bigint,
        @After_DerivCnt  bigint;

    /* ========= BEGIN LOG ========= */
    INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
        (ScriptName, LaunchTimestamp, StartTime, Message, Element, Complement)
    VALUES
        (@ScriptName, @LaunchTs, @LaunchTs, N'BEGIN', N'RUN', NULL);

    /* ========= Resolve last FULL ========= */
    SELECT @LastFullValidFromDate = MAX(ValidFromDate)
    FROM stg.ESMA_FULINS_WIDE
    WHERE SourceFileName LIKE N'FULINS_%';

    IF @LastFullValidFromDate IS NULL
    BEGIN
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
            (ScriptName, LaunchTimestamp, EndTime, Message, Element, Complement)
        VALUES
            (@ScriptName, @LaunchTs, SYSDATETIME(),
             N'SKIP - no FULL found in stg.ESMA_FULINS_WIDE', N'SKIP',
             N'LastFullValidFromDate=NULL');

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
            (ScriptName, LaunchTimestamp, EndTime, Message, Element, Complement)
        VALUES
            (@ScriptName, @LaunchTs, SYSDATETIME(), N'END', N'RUN', NULL);

        RETURN;
    END;

    /* ========= BEFORE snapshot (counts) ========= */
    SELECT @Before_ListCnt  = COUNT_BIG(1) FROM stg.ESMA_INSTRUMENT_LISTING;
    SELECT @Before_DebtCnt  = COUNT_BIG(1) FROM stg.ESMA_INSTRUMENT_DEBT;
    SELECT @Before_DerivCnt = COUNT_BIG(1) FROM stg.ESMA_INSTRUMENT_DERIVATIVE;

    INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
        (ScriptName, LaunchTimestamp, StartTime, Message, Element, Complement)
    VALUES
        (@ScriptName, @LaunchTs, SYSDATETIME(),
         N'BEFORE - instrument rebuild snapshot', N'BEFORE',
         CONCAT(
             N'LastFullValidFromDate=', CONVERT(varchar(10), @LastFullValidFromDate, 120),
             N' | LISTING=', @Before_ListCnt,
             N' | DEBT=', @Before_DebtCnt,
             N' | DERIVATIVE=', @Before_DerivCnt
         ));

    BEGIN TRY
        BEGIN TRAN;

        IF OBJECT_ID('tempdb..#base_dedup') IS NOT NULL DROP TABLE #base_dedup;
        
        ;WITH base_full AS (
            SELECT 
                HeaderReportingMarketId, HeaderReportingNCA, HeaderReportingPeriodDate, SourceFileName, TechRcrdId, ISIN, FullName, ShortName, CFI, CommodityDerivativeInd, NotionalCurrency, IssuerLEI, TradingVenueMIC, IssuerReqAdmission, AdmissionApprvlDate, ReqForAdmissionDate, FirstTradingDate, TerminationDate, TotalIssuedNominalAmount, TotalIssuedNominalAmountCcy, MaturityDate, NominalValuePerUnit, NominalValuePerUnitCcy, FixedRate, FloatRefRateISIN, FloatRefRateIndex, FloatTermUnit, FloatTermValue, FloatBasisPointSpread, DebtSeniority, ExpiryDate, PriceMultiplier, UnderlyingISIN, UnderlyingLEI, UnderlyingIndexRef, UnderlyingIndexTermUnit, UnderlyingIndexTermValue, OptionType, OptionExerciseStyle, DeliveryType, StrikePrice, StrikePriceCcy, StrikeNoPriceCcy, CmdtyBaseProduct, CmdtySubProduct, CmdtySubSubProduct, CmdtyTransactionType, CmdtyFinalPriceType, ValidFromDate, ValidToDate, LatestRecordFlag
            FROM (
                SELECT HeaderReportingMarketId, HeaderReportingNCA, HeaderReportingPeriodDate, SourceFileName, TechRcrdId, ISIN, FullName, ShortName, CFI, CommodityDerivativeInd, NotionalCurrency, IssuerLEI, TradingVenueMIC, IssuerReqAdmission, AdmissionApprvlDate, ReqForAdmissionDate, FirstTradingDate, TerminationDate, TotalIssuedNominalAmount, TotalIssuedNominalAmountCcy, MaturityDate, NominalValuePerUnit, NominalValuePerUnitCcy, FixedRate, FloatRefRateISIN, FloatRefRateIndex, FloatTermUnit, FloatTermValue, FloatBasisPointSpread, DebtSeniority, ExpiryDate, PriceMultiplier, UnderlyingISIN, UnderlyingLEI, UnderlyingIndexRef, UnderlyingIndexTermUnit, UnderlyingIndexTermValue, OptionType, OptionExerciseStyle, DeliveryType, StrikePrice, StrikePriceCcy, StrikeNoPriceCcy, CmdtyBaseProduct, CmdtySubProduct, CmdtySubSubProduct, CmdtyTransactionType, CmdtyFinalPriceType, ValidFromDate, ValidToDate, LatestRecordFlag
                    , ROW_NUMBER() OVER (PARTITION BY ISIN, IssuerLEI, TradingVenueMIC ORDER BY ValidFromDate DESC) rnk
                FROM [stg].[ESMA_FULINS_WIDE] a   
                WHERE LatestRecordFlag = 1
            ) a
            WHERE rnk = 1
        ),
        base_typed AS (
            SELECT
                ISIN_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),ISIN))),N''),
                MIC_s  = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),TradingVenueMIC))),N''),
                SourceFileName_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),SourceFileName))),N''),
                TechRcrdId_s     = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),TechRcrdId))),N''),
                FullName_s       = NULLIF(CONVERT(nvarchar(400),FullName),N''),
                ShortName_s      = NULLIF(CONVERT(nvarchar(200),ShortName),N''),
                CFI_s            = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CFI))),N''),

                HeaderReportingMarketId_s   = NULLIF(CONVERT(nvarchar(255),HeaderReportingMarketId),N''),
                HeaderReportingNCA_s        = NULLIF(CONVERT(nvarchar(255),HeaderReportingNCA),N''),
                HeaderReportingPeriodDate_d = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),HeaderReportingPeriodDate),N'')),

                ValidFromDate_d    = ValidFromDate,
                ValidToDate_d      = ValidToDate,
                LatestRecordFlag_b = LatestRecordFlag,
                ValidFromDate_PK_d = COALESCE(ValidFromDate, CONVERT(date,'19000101')),

                CommodityDerivativeInd_b =
                    CASE LOWER(NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CommodityDerivativeInd))),N''))
                        WHEN '1' THEN 1 WHEN 'true' THEN 1 WHEN 't' THEN 1 WHEN 'y' THEN 1 WHEN 'yes' THEN 1
                        WHEN '0' THEN 0 WHEN 'false' THEN 0 WHEN 'f' THEN 0 WHEN 'n' THEN 0 WHEN 'no' THEN 0
                        ELSE NULL
                    END,
                NotionalCurrency_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),NotionalCurrency))),N''),
                IssuerLEI_s        = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),IssuerLEI))),N''),
                IssuerReqAdmission_b =
                    CASE LOWER(NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),IssuerReqAdmission))),N''))
                        WHEN '1' THEN 1 WHEN 'true' THEN 1 WHEN 't' THEN 1 WHEN 'y' THEN 1 WHEN 'yes' THEN 1
                        WHEN '0' THEN 0 WHEN 'false' THEN 0 WHEN 'f' THEN 0 WHEN 'n' THEN 0 WHEN 'no' THEN 0
                        ELSE NULL
                    END,
                AdmissionApprvlDate_d = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),AdmissionApprvlDate),N'')),
                ReqForAdmissionDate_d = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),ReqForAdmissionDate),N'')),
                FirstTradingDate_d    = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),FirstTradingDate),N'')),
                TerminationDate_d     = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),TerminationDate),N'')),

                CmdtyBaseProduct_s     = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CmdtyBaseProduct))),N''),
                CmdtySubProduct_s      = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CmdtySubProduct))),N''),
                CmdtySubSubProduct_s   = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CmdtySubSubProduct))),N''),
                CmdtyTransactionType_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CmdtyTransactionType))),N''),
                CmdtyFinalPriceType_s  = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),CmdtyFinalPriceType))),N''),

                TotalIssuedNominalAmount_n = TRY_CONVERT(decimal(38,10), REPLACE(NULLIF(CONVERT(nvarchar(255),TotalIssuedNominalAmount),N''),',','.')),
                TotalIssuedNominalAmountCcy_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),TotalIssuedNominalAmountCcy))),N''),
                MaturityDate_d = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),MaturityDate),N'')),
                NominalValuePerUnit_n = TRY_CONVERT(decimal(38,10), REPLACE(NULLIF(CONVERT(nvarchar(255),NominalValuePerUnit),N''),',','.')),
                NominalValuePerUnitCcy_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),NominalValuePerUnitCcy))),N''),
                FixedRate_n = TRY_CONVERT(decimal(38,10), REPLACE(NULLIF(CONVERT(nvarchar(255),FixedRate),N''),',','.')),
                FloatRefRateISIN_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),FloatRefRateISIN))),N''),
                FloatRefRateIndex_s = NULLIF(CONVERT(nvarchar(255),FloatRefRateIndex),N''),
                FloatTermUnit_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),FloatTermUnit))),N''),
                FloatTermValue_i = TRY_CONVERT(int, NULLIF(CONVERT(nvarchar(255),FloatTermValue),N'')),
                FloatBasisPointSpread_n = TRY_CONVERT(decimal(38,10), REPLACE(NULLIF(CONVERT(nvarchar(255),FloatBasisPointSpread),N''),',','.')),
                DebtSeniority_s = NULLIF(CONVERT(nvarchar(100),DebtSeniority),N''),

                ExpiryDate_d = TRY_CONVERT(date, NULLIF(CONVERT(nvarchar(255),ExpiryDate),N'')),
                PriceMultiplier_n = TRY_CONVERT(decimal(38,10), REPLACE(NULLIF(CONVERT(nvarchar(255),PriceMultiplier),N''),',','.')),
                UnderlyingISIN_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),UnderlyingISIN))),N''),
                UnderlyingLEI_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),UnderlyingLEI))),N''),
                UnderlyingIndexRef_s = NULLIF(CONVERT(nvarchar(255),UnderlyingIndexRef),N''),
                UnderlyingIndexTermUnit_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),UnderlyingIndexTermUnit))),N''),
                UnderlyingIndexTermValue_i = TRY_CONVERT(int, NULLIF(CONVERT(nvarchar(255),UnderlyingIndexTermValue),N'')),
                OptionType_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),OptionType))),N''),
                OptionExerciseStyle_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),OptionExerciseStyle))),N''),
                DeliveryType_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),DeliveryType))),N''),
                StrikePrice_n = TRY_CONVERT(decimal(38,10), REPLACE(NULLIF(CONVERT(nvarchar(255),StrikePrice),N''),',','.')),
                StrikePriceCcy_s = NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),StrikePriceCcy))),N''),
                StrikeNoPriceCcy_b =
                    CASE LOWER(NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255),StrikeNoPriceCcy))),N''))
                        WHEN '1' THEN 1 WHEN 'true' THEN 1 WHEN 't' THEN 1 WHEN 'y' THEN 1 WHEN 'yes' THEN 1
                        WHEN '0' THEN 0 WHEN 'false' THEN 0 WHEN 'f' THEN 0 WHEN 'n' THEN 0 WHEN 'no' THEN 0
                        ELSE NULL
                    END
            FROM base_full
        )
        SELECT *
        INTO #base_dedup
        FROM base_typed;

        /* LISTING */
        TRUNCATE TABLE stg.ESMA_INSTRUMENT_LISTING;

        INSERT INTO stg.ESMA_INSTRUMENT_LISTING (
            ISIN, TradingVenueMIC,
            ValidFromDate, ValidToDate, LatestRecordFlag, ValidFromDate_PK,
            FullName, ShortName, CFI,
            CommodityDerivativeInd, NotionalCurrency, IssuerLEI,
            IssuerReqAdmission, AdmissionApprvlDate, ReqForAdmissionDate,
            FirstTradingDate, TerminationDate,
            CmdtyBaseProduct, CmdtySubProduct, CmdtySubSubProduct, CmdtyTransactionType, CmdtyFinalPriceType,
            HeaderReportingMarketId, HeaderReportingNCA, HeaderReportingPeriodDate,
            SourceFileName, TechRcrdId
        )
        SELECT
            CONVERT(varchar(12), ISIN_s),
            CONVERT(varchar(50), MIC_s),
            ValidFromDate_d,
            ValidToDate_d,
            LatestRecordFlag_b,
            ValidFromDate_PK_d,
            FullName_s,
            ShortName_s,
            CONVERT(varchar(6), CFI_s),
            CommodityDerivativeInd_b,
            CONVERT(varchar(10), NotionalCurrency_s),
            CONVERT(varchar(50), IssuerLEI_s),
            IssuerReqAdmission_b,
            AdmissionApprvlDate_d,
            ReqForAdmissionDate_d,
            FirstTradingDate_d,
            TerminationDate_d,
            CONVERT(varchar(50), CmdtyBaseProduct_s),
            CONVERT(varchar(50), CmdtySubProduct_s),
            CONVERT(varchar(50), CmdtySubSubProduct_s),
            CONVERT(varchar(50), CmdtyTransactionType_s),
            CONVERT(varchar(50), CmdtyFinalPriceType_s),
            HeaderReportingMarketId_s,
            HeaderReportingNCA_s,
            HeaderReportingPeriodDate_d,
            CONVERT(nvarchar(260), SourceFileName_s),
            TechRcrdId_s
        FROM #base_dedup;

        /* DEBT */
        TRUNCATE TABLE stg.ESMA_INSTRUMENT_DEBT;

        INSERT INTO stg.ESMA_INSTRUMENT_DEBT (
            ISIN, TradingVenueMIC,
            ValidFromDate, ValidToDate, LatestRecordFlag, ValidFromDate_PK,
            TotalIssuedNominalAmount, TotalIssuedNominalAmountCcy,
            MaturityDate, NominalValuePerUnit, NominalValuePerUnitCcy,
            FixedRate, FloatRefRateISIN, FloatRefRateIndex,
            FloatTermUnit, FloatTermValue, FloatBasisPointSpread, DebtSeniority
        )
        SELECT
            CONVERT(varchar(12), ISIN_s),
            CONVERT(varchar(50), MIC_s),
            ValidFromDate_d,
            ValidToDate_d,
            LatestRecordFlag_b,
            ValidFromDate_PK_d,
            TotalIssuedNominalAmount_n,
            CONVERT(varchar(10), TotalIssuedNominalAmountCcy_s),
            MaturityDate_d,
            NominalValuePerUnit_n,
            CONVERT(varchar(10), NominalValuePerUnitCcy_s),
            FixedRate_n,
            CONVERT(varchar(12), FloatRefRateISIN_s),
            FloatRefRateIndex_s,
            CONVERT(varchar(10), FloatTermUnit_s),
            FloatTermValue_i,
            FloatBasisPointSpread_n,
            DebtSeniority_s
        FROM #base_dedup
        WHERE CFI_s IS NOT NULL AND LEFT(CFI_s,1) = 'D';

        /* DERIVATIVE */
        TRUNCATE TABLE stg.ESMA_INSTRUMENT_DERIVATIVE;

        INSERT INTO stg.ESMA_INSTRUMENT_DERIVATIVE (
            ISIN, TradingVenueMIC,
            ValidFromDate, ValidToDate, LatestRecordFlag, ValidFromDate_PK,
            ExpiryDate, PriceMultiplier,
            UnderlyingISIN, UnderlyingLEI,
            UnderlyingIndexRef, UnderlyingIndexTermUnit, UnderlyingIndexTermValue,
            OptionType, OptionExerciseStyle, DeliveryType,
            StrikePrice, StrikePriceCcy, StrikeNoPriceCcy,
            CmdtyBaseProduct, CmdtySubProduct, CmdtySubSubProduct, CmdtyTransactionType, CmdtyFinalPriceType
        )
        SELECT
            CONVERT(varchar(12), ISIN_s),
            CONVERT(varchar(50), MIC_s),
            ValidFromDate_d,
            ValidToDate_d,
            LatestRecordFlag_b,
            ValidFromDate_PK_d,
            ExpiryDate_d,
            PriceMultiplier_n,
            CONVERT(varchar(12), UnderlyingISIN_s),
            CONVERT(varchar(50), UnderlyingLEI_s),
            UnderlyingIndexRef_s,
            CONVERT(varchar(10), UnderlyingIndexTermUnit_s),
            UnderlyingIndexTermValue_i,
            CONVERT(varchar(50), OptionType_s),
            CONVERT(varchar(50), OptionExerciseStyle_s),
            CONVERT(varchar(50), DeliveryType_s),
            StrikePrice_n,
            CONVERT(varchar(10), StrikePriceCcy_s),
            StrikeNoPriceCcy_b,
            CONVERT(varchar(50), CmdtyBaseProduct_s),
            CONVERT(varchar(50), CmdtySubProduct_s),
            CONVERT(varchar(50), CmdtySubSubProduct_s),
            CONVERT(varchar(50), CmdtyTransactionType_s),
            CONVERT(varchar(50), CmdtyFinalPriceType_s)
        FROM #base_dedup
        WHERE
            (CFI_s IS NOT NULL AND LEFT(CFI_s,1) IN ('F','O','S'))
            OR CommodityDerivativeInd_b = 1
            OR CmdtyBaseProduct_s IS NOT NULL;

        DROP TABLE #base_dedup;

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        IF OBJECT_ID('tempdb..#base_dedup') IS NOT NULL DROP TABLE #base_dedup;

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
            (ScriptName, LaunchTimestamp, EndTime, Message, Element, Complement)
        VALUES
            (@ScriptName, @LaunchTs, SYSDATETIME(),
             N'ERROR', N'EXCEPTION',
             CONCAT(N'LastFullValidFromDate=',CONVERT(varchar(10),@LastFullValidFromDate,120),
                    N' | ', ERROR_MESSAGE()));

        THROW;
    END CATCH;

    /* ========= AFTER snapshot (counts) ========= */
    SELECT @After_ListCnt  = COUNT_BIG(1) FROM stg.ESMA_INSTRUMENT_LISTING;
    SELECT @After_DebtCnt  = COUNT_BIG(1) FROM stg.ESMA_INSTRUMENT_DEBT;
    SELECT @After_DerivCnt = COUNT_BIG(1) FROM stg.ESMA_INSTRUMENT_DERIVATIVE;

    INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
        (ScriptName, LaunchTimestamp, EndTime, Message, Element, Complement)
    VALUES
        (@ScriptName, @LaunchTs, SYSDATETIME(),
         N'AFTER - instrument rebuild snapshot', N'AFTER',
         CONCAT(
             N'LastFullValidFromDate=', CONVERT(varchar(10), @LastFullValidFromDate, 120),
             N' | LISTING=', @After_ListCnt,
             N' | DEBT=', @After_DebtCnt,
             N' | DERIVATIVE=', @After_DerivCnt
         ));

    /* ========= END LOG ========= */
    INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log]
        (ScriptName, LaunchTimestamp, EndTime, Message, Element, Complement)
    VALUES
        (@ScriptName, @LaunchTs, SYSDATETIME(), N'END', N'RUN', NULL);
END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON

/* -----------------------------------------------------------------------------
   Delta apply proc (NO BULK INSERT)
   ---------------------------------------------------------------------------- */
CREATE   PROCEDURE [stg].[usp_Process_DLTINS_Daily]
   -- @AsOfDate date
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc nvarchar(200) = N'stg.usp_Process_DLTINS_Daily';
    DECLARE @step nvarchar(200) = N'START';
    DECLARE @horodatage datetime2(0)=SYSUTCDATETIME();
    BEGIN TRY
        EXEC log.usp_ESMA_WriteLog
            @ProcessName=@proc,
            @StepName=@step,
            @LogLevel=N'INFO',
            @Message=N'START apply DLTINS -> FULINS (no bulk)',
            @EventUTC=@horodatage,
            @RowCount=NULL,
            @DetailsJson=NULL;

     ----------update date ValidFromDate for ESMA_DLTINS_WIDE
	begin transaction update_ESMA_DLTINS
	update  stg.ESMA_DLTINS_WIDE  set ValidFromDate=TRY_CONVERT(date, LEFT(coalesce(ValidFromDate,HeaderReportingPeriodDate), 10))
	COMMIT TRANSACTION update_ESMA_DLTINS 
	    IF OBJECT_ID('tempdb..#D') IS NOT NULL DROP TABLE #D;

        ;WITH D0 AS (
            SELECT
                d.*,
                VF   = TRY_CONVERT(date, LEFT(coalesce(d.ValidFromDate,d.HeaderReportingPeriodDate), 10)),
                TERM = TRY_CONVERT(date, LEFT(d.TerminationDate, 10)),
                RN = ROW_NUMBER() OVER (
                    PARTITION BY d.ISIN, d.TradingVenueMIC, LEFT(d.ValidFromDate,10), d.ActionType
                    ORDER BY TRY_CONVERT(bigint, d.TechRcrdId) DESC
                )
            FROM stg.ESMA_DLTINS_WIDE d
            WHERE d.ISIN IS NOT NULL
              AND d.TradingVenueMIC IS NOT NULL
        )
        SELECT *
        INTO #D
        FROM D0
        WHERE RN = 1 AND VF IS NOT NULL;

        CREATE INDEX IX_D_BK ON #D (ISIN, TradingVenueMIC, VF, ActionType);

        /* MOD: close current at VF-1 */
        SET @step = N'CLOSE MOD';
        UPDATE f
            SET f.ValidToDate = CONVERT(nvarchar(10), DATEADD(day, -1, d.VF), 23),
                f.LatestRecordFlag = 0
        FROM stg.ESMA_FULINS_WIDE f
        JOIN #D d
          ON d.ActionType = 'MOD'
         AND f.ISIN = d.ISIN
         AND f.TradingVenueMIC = d.TradingVenueMIC
        WHERE f.ValidToDate IS NULL
          AND f.LatestRecordFlag = 1;

        /* TERM: close current at TerminationDate (if present) else VF */
        SET @step = N'CLOSE TERM';
        UPDATE f
            SET f.ValidToDate = CONVERT(nvarchar(10), COALESCE(d.TERM, d.VF), 23),
                f.LatestRecordFlag = 0
        FROM stg.ESMA_FULINS_WIDE f
        JOIN #D d
          ON d.ActionType = 'TERM'
         AND f.ISIN = d.ISIN
         AND f.TradingVenueMIC = d.TradingVenueMIC
        WHERE f.ValidToDate IS NULL
          AND f.LatestRecordFlag = 1;

        /* CANC: close current at VF */
        SET @step = N'CLOSE CANC';
        UPDATE f
            SET f.ValidToDate = CONVERT(nvarchar(10), d.VF, 23),
                f.LatestRecordFlag = 0
        FROM stg.ESMA_FULINS_WIDE f
        JOIN #D d
          ON d.ActionType = 'CANC'
         AND f.ISIN = d.ISIN
         AND f.TradingVenueMIC = d.TradingVenueMIC
        WHERE f.ValidToDate IS NULL
          AND f.LatestRecordFlag = 1;

        /* Insert NEW + MOD rows (TERM is close-only in this daily apply) */
        SET @step = N'INSERT NEW/MOD';

        DECLARE @commonCols nvarchar(max);

        SELECT @commonCols = STUFF((
            SELECT N',' + QUOTENAME(fcol.name)
            FROM sys.columns fcol
            INNER JOIN sys.columns dcol
                ON dcol.name = fcol.name
               AND dcol.object_id = OBJECT_ID('stg.ESMA_DLTINS_WIDE')
            WHERE fcol.object_id = OBJECT_ID('stg.ESMA_FULINS_WIDE')
              AND fcol.name NOT IN ('ValidToDate','LatestRecordFlag')
            ORDER BY fcol.column_id
            FOR XML PATH(''), TYPE
        ).value('.','nvarchar(max)'), 1, 1, N'');

        IF @commonCols IS NULL OR LTRIM(RTRIM(@commonCols)) = N''
            THROW 50001, 'No common columns found between stg.ESMA_DLTINS_WIDE and stg.ESMA_FULINS_WIDE.', 1;

        DECLARE @insSql nvarchar(max);
        SET @insSql =
            N'INSERT INTO stg.ESMA_FULINS_WIDE (' + @commonCols + N',[ValidToDate],[LatestRecordFlag]) ' +
            N'SELECT ' + @commonCols + N', NULL, 1 ' +
            N'FROM #D d ' +
            N'WHERE d.ActionType IN (''NEW'',''MOD'') ' +
            N'AND NOT EXISTS ( ' +
            N'  SELECT 1 FROM stg.ESMA_FULINS_WIDE f ' +
            N'  WHERE f.ISIN = d.ISIN ' +
            N'    AND f.TradingVenueMIC = d.TradingVenueMIC ' +
            N'    AND TRY_CONVERT(date, LEFT(f.ValidFromDate,10)) = d.VF ' +
            N');';

        EXEC(@insSql);

        SET @step = N'DONE';
        DECLARE @openCnt int = (SELECT COUNT(*) FROM stg.ESMA_FULINS_WIDE WHERE LatestRecordFlag = 1 AND ValidToDate IS NULL);
         DECLARE @horodatage2 datetime2(0)=SYSUTCDATETIME();
        EXEC log.usp_ESMA_WriteLog
            @ProcessName=@proc,
            @StepName=@step,
            @LogLevel=N'INFO',
            @Message=N'DONE apply DLTINS -> FULINS (no bulk)',
            @EventUTC=@horodatage2,
            @RowCount=@openCnt,
            @DetailsJson=NULL;

    END TRY
    BEGIN CATCH
    declare @err_num int =ERROR_NUMBER();
        declare @err_line int =ERROR_LINE();
       declare @err_msg varchar(200) =ERROR_MESSAGE();
       declare  @log_msg varchar(300)=CONCAT(N'|ERROR :', @err_num,N' line: ',  @err_line, N'Message: ', @err_msg);
        EXEC log.usp_ESMA_WriteLog
            @ProcessName=@proc,
            @StepName=N'FAILED',
            @LogLevel=N'ERROR',
            @Message=@log_msg,
            @EventUTC=@horodatage2,
            @RowCount=NULL,
            @DetailsJson=NULL;
        THROW;
    END CATCH
END

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF

CREATE PROCEDURE [stg].[usp_Run_Daily_stg_Load]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LaunchTs datetime2(0) = SYSDATETIME();
    DECLARE @ScriptName nvarchar(255) = N'stg.usp_Run_Daily_stg_Load';

    DECLARE @StartStep datetime2(0);
    DECLARE @EndStep datetime2(0);

    DECLARE @rc_before bigint, @rc_after bigint;

    BEGIN TRY
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element])
        VALUES (@ScriptName,@LaunchTs,@LaunchTs,N'START',N'RUN');

        -------------------------------------------------------------------------
        -- Step 1: stg.usp_Process_DLTINS_Daily
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();

        SELECT @rc_before = COALESCE(SUM(row_count),0)
        FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'stg.ESMA_FULINS_WIDE') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC stg.usp_Process_DLTINS_Daily (before)',N'stg.ESMA_FULINS_WIDE',CONCAT(N'rowcount_before=',@rc_before));

        EXEC [stg].[usp_Process_DLTINS_Daily];
        EXEC [stg].[usp_Load_ESMA_INSTRUMENTS_From_FULINS_WIDE];
        
        SELECT @rc_after = COALESCE(SUM(row_count),0)
        FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'stg.ESMA_FULINS_WIDE') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC stg.usp_Process_DLTINS_Daily (after)',N'stg.ESMA_FULINS_WIDE',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));
     
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[EndTime],[Message],[Element])
        VALUES (@ScriptName,@LaunchTs,SYSDATETIME(),N'END',N'RUN');
    END TRY
    BEGIN CATCH
        DECLARE @Err nvarchar(4000) = ERROR_MESSAGE();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,SYSDATETIME(),N'ERROR',N'EXCEPTION',@Err);
        THROW;
    END CATCH
END;

