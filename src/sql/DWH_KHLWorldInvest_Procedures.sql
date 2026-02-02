/****** Cannot script Unresolved Entities : Server[@Name='PERSO-AJE-DELL\MSSQLSERVER01']/Database[@Name='DWH_KHLWorldInvest']/UnresolvedEntity[@Name='cur'] ******/
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [log].[ESMA_Load_Log](
	[LogID] [int] IDENTITY(1,1) NOT NULL,
	[ScriptName] [nvarchar](255) COLLATE French_CI_AS NOT NULL,
	[LaunchTimestamp] [datetime2](0) NOT NULL,
	[StartTime] [datetime2](0) NULL,
	[EndTime] [datetime2](0) NULL,
	[Message] [nvarchar](4000) COLLATE French_CI_AS NULL,
	[FileName] [nvarchar](400) COLLATE French_CI_AS NULL,
	[Element] [nvarchar](255) COLLATE French_CI_AS NULL,
	[Complement] [nvarchar](1000) COLLATE French_CI_AS NULL,
	[CreatedOn] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_ESMA_Load_Log] PRIMARY KEY CLUSTERED 
(
	[LogID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[ESMA_INSTRUMENT_LISTING](
	[ISIN] [varchar](12) COLLATE French_CI_AS NOT NULL,
	[TradingVenueMIC] [varchar](50) COLLATE French_CI_AS NOT NULL,
	[ValidFromDate] [date] NULL,
	[ValidToDate] [date] NULL,
	[LatestRecordFlag] [bit] NULL,
	[ValidFromDate_PK] [date] NOT NULL,
	[FullName] [nvarchar](400) COLLATE French_CI_AS NULL,
	[ShortName] [nvarchar](200) COLLATE French_CI_AS NULL,
	[CFI] [varchar](6) COLLATE French_CI_AS NULL,
	[CommodityDerivativeInd] [bit] NULL,
	[NotionalCurrency] [varchar](10) COLLATE French_CI_AS NULL,
	[IssuerLEI] [varchar](50) COLLATE French_CI_AS NULL,
	[IssuerReqAdmission] [bit] NULL,
	[AdmissionApprvlDate] [date] NULL,
	[ReqForAdmissionDate] [date] NULL,
	[FirstTradingDate] [date] NULL,
	[TerminationDate] [date] NULL,
	[CmdtyBaseProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubSubProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtyTransactionType] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtyFinalPriceType] [varchar](50) COLLATE French_CI_AS NULL,
	[HeaderReportingMarketId] [nvarchar](255) COLLATE French_CI_AS NULL,
	[HeaderReportingNCA] [nvarchar](255) COLLATE French_CI_AS NULL,
	[HeaderReportingPeriodDate] [date] NULL,
	[SourceFileName] [nvarchar](260) COLLATE French_CI_AS NULL,
	[TechRcrdId] [nvarchar](255) COLLATE French_CI_AS NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_stg_ESMA_INSTRUMENT_LISTING] PRIMARY KEY CLUSTERED 
(
	[ISIN] ASC,
	[TradingVenueMIC] ASC,
	[ValidFromDate_PK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[ESMA_INSTRUMENT_DERIVATIVE](
	[ISIN] [varchar](12) COLLATE French_CI_AS NOT NULL,
	[TradingVenueMIC] [varchar](50) COLLATE French_CI_AS NOT NULL,
	[ValidFromDate] [date] NULL,
	[ValidToDate] [date] NULL,
	[LatestRecordFlag] [bit] NULL,
	[ValidFromDate_PK] [date] NOT NULL,
	[ExpiryDate] [date] NULL,
	[PriceMultiplier] [decimal](38, 10) NULL,
	[UnderlyingISIN] [varchar](12) COLLATE French_CI_AS NULL,
	[UnderlyingLEI] [varchar](50) COLLATE French_CI_AS NULL,
	[UnderlyingIndexRef] [nvarchar](255) COLLATE French_CI_AS NULL,
	[UnderlyingIndexTermUnit] [varchar](10) COLLATE French_CI_AS NULL,
	[UnderlyingIndexTermValue] [int] NULL,
	[OptionType] [varchar](50) COLLATE French_CI_AS NULL,
	[OptionExerciseStyle] [varchar](50) COLLATE French_CI_AS NULL,
	[DeliveryType] [varchar](50) COLLATE French_CI_AS NULL,
	[StrikePrice] [decimal](38, 10) NULL,
	[StrikePriceCcy] [varchar](10) COLLATE French_CI_AS NULL,
	[StrikeNoPriceCcy] [bit] NULL,
	[CmdtyBaseProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubSubProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtyTransactionType] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtyFinalPriceType] [varchar](50) COLLATE French_CI_AS NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_stg_ESMA_INSTRUMENT_DERIVATIVE] PRIMARY KEY CLUSTERED 
(
	[ISIN] ASC,
	[TradingVenueMIC] ASC,
	[ValidFromDate_PK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[ESMA_INSTRUMENT_DEBT](
	[ISIN] [varchar](12) COLLATE French_CI_AS NOT NULL,
	[TradingVenueMIC] [varchar](50) COLLATE French_CI_AS NOT NULL,
	[ValidFromDate] [date] NULL,
	[ValidToDate] [date] NULL,
	[LatestRecordFlag] [bit] NULL,
	[ValidFromDate_PK] [date] NOT NULL,
	[TotalIssuedNominalAmount] [decimal](38, 10) NULL,
	[TotalIssuedNominalAmountCcy] [varchar](10) COLLATE French_CI_AS NULL,
	[MaturityDate] [date] NULL,
	[NominalValuePerUnit] [decimal](38, 10) NULL,
	[NominalValuePerUnitCcy] [varchar](10) COLLATE French_CI_AS NULL,
	[FixedRate] [decimal](38, 10) NULL,
	[FloatRefRateISIN] [varchar](12) COLLATE French_CI_AS NULL,
	[FloatRefRateIndex] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FloatTermUnit] [varchar](10) COLLATE French_CI_AS NULL,
	[FloatTermValue] [int] NULL,
	[FloatBasisPointSpread] [decimal](38, 10) NULL,
	[DebtSeniority] [nvarchar](100) COLLATE French_CI_AS NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_stg_ESMA_INSTRUMENT_DEBT] PRIMARY KEY CLUSTERED 
(
	[ISIN] ASC,
	[TradingVenueMIC] ASC,
	[ValidFromDate_PK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[STG_LEI_CDF_GOLDEN](
	[LEI] [nvarchar](50) COLLATE French_CI_AS NOT NULL,
	[LegalName] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalName_xmllang] [nvarchar](50) COLLATE French_CI_AS NULL,
	[OtherEntityName_1] [nvarchar](500) COLLATE French_CI_AS NULL,
	[OtherEntityName_1_xmllang] [nvarchar](50) COLLATE French_CI_AS NULL,
	[OtherEntityName_1_type] [nvarchar](50) COLLATE French_CI_AS NULL,
	[OtherEntityName_2] [nvarchar](500) COLLATE French_CI_AS NULL,
	[OtherEntityName_2_xmllang] [nvarchar](50) COLLATE French_CI_AS NULL,
	[OtherEntityName_2_type] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalAddress_xmllang] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalAddress_FirstAddressLine] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalAddress_AdditionalAddressLine_1] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalAddress_AdditionalAddressLine_2] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalAddress_AdditionalAddressLine_3] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalAddress_City] [nvarchar](200) COLLATE French_CI_AS NULL,
	[LegalAddress_Region] [nvarchar](200) COLLATE French_CI_AS NULL,
	[LegalAddress_Country] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalAddress_PostalCode] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_xmllang] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_FirstAddressLine] [nvarchar](500) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_MailRouting] [nvarchar](200) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_AdditionalAddressLine_1] [nvarchar](500) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_AdditionalAddressLine_2] [nvarchar](500) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_AdditionalAddressLine_3] [nvarchar](500) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_City] [nvarchar](200) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_Region] [nvarchar](200) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_Country] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_PostalCode] [nvarchar](50) COLLATE French_CI_AS NULL,
	[RegistrationAuthority_RegistrationAuthorityID] [nvarchar](200) COLLATE French_CI_AS NULL,
	[RegistrationAuthority_OtherRegistrationAuthorityID] [nvarchar](200) COLLATE French_CI_AS NULL,
	[RegistrationAuthority_RegistrationAuthorityEntityID] [nvarchar](200) COLLATE French_CI_AS NULL,
	[LegalJurisdiction] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EntityCategory] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EntitySubCategory] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalForm_EntityLegalFormCode] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalForm_OtherLegalForm] [nvarchar](500) COLLATE French_CI_AS NULL,
	[AssociatedEntity_type] [nvarchar](50) COLLATE French_CI_AS NULL,
	[AssociatedEntity_AssociatedLEI] [char](20) COLLATE French_CI_AS NULL,
	[AssociatedEntity_AssociatedEntityName] [nvarchar](500) COLLATE French_CI_AS NULL,
	[AssociatedEntity_AssociatedEntityName_xmllang] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EntityStatus] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EntityCreationDate] [datetime2](0) NULL,
	[LegalEntityEvent_1_group_type] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_1_event_status] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_1_LegalEntityEventType] [nvarchar](100) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_1_LegalEntityEventEffectiveDate] [datetime2](0) NULL,
	[LegalEntityEvent_1_LegalEntityEventRecordedDate] [datetime2](0) NULL,
	[LegalEntityEvent_1_ValidationDocuments] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_2_group_type] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_2_event_status] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_2_LegalEntityEventType] [nvarchar](100) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_2_LegalEntityEventEffectiveDate] [datetime2](0) NULL,
	[LegalEntityEvent_2_LegalEntityEventRecordedDate] [datetime2](0) NULL,
	[LegalEntityEvent_2_ValidationDocuments] [nvarchar](500) COLLATE French_CI_AS NULL,
	[InitialRegistrationDate] [datetime2](0) NULL,
	[LastUpdateDate] [datetime2](0) NULL,
	[RegistrationStatus] [nvarchar](50) COLLATE French_CI_AS NULL,
	[NextRenewalDate] [datetime2](0) NULL,
	[ManagingLOU] [char](20) COLLATE French_CI_AS NULL,
	[ValidationSources] [nvarchar](100) COLLATE French_CI_AS NULL,
	[ValidationAuthority_ValidationAuthorityID] [nvarchar](200) COLLATE French_CI_AS NULL,
	[ValidationAuthority_OtherValidationAuthorityID] [nvarchar](200) COLLATE French_CI_AS NULL,
	[ValidationAuthority_ValidationAuthorityEntityID] [nvarchar](200) COLLATE French_CI_AS NULL,
	[ConformityFlag] [nvarchar](50) COLLATE French_CI_AS NULL
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[DimTradingVenue](
	[TradingVenueSK] [int] IDENTITY(1,1) NOT NULL,
	[TradingVenueMIC] [nvarchar](10) COLLATE French_CI_AS NOT NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_DimTradingVenue] PRIMARY KEY CLUSTERED 
(
	[TradingVenueSK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[DimIssuer](
	[IssuerSK] [int] IDENTITY(1,1) NOT NULL,
	[IssuerLEI] [nvarchar](20) COLLATE French_CI_AS NOT NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
	[LegalName] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalAddress_FirstAddressLine] [nvarchar](500) COLLATE French_CI_AS NULL,
	[LegalAddress_City] [nvarchar](200) COLLATE French_CI_AS NULL,
	[LegalAddress_Region] [nvarchar](200) COLLATE French_CI_AS NULL,
	[LegalAddress_Country] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalAddress_PostalCode] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_FirstAddressLine] [nvarchar](500) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_City] [nvarchar](200) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_Country] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeadquartersAddress_PostalCode] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalJurisdiction] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EntityCategory] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EntityStatus] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LegalEntityEvent_1_group_type] [nvarchar](50) COLLATE French_CI_AS NULL,
	[InitialRegistrationDate] [datetime2](0) NULL,
	[LastUpdateDate] [datetime2](0) NULL,
 CONSTRAINT [PK_DimIssuer] PRIMARY KEY CLUSTERED 
(
	[IssuerSK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[DimInstrumentListing_SCD2](
	[InstrumentListingSK] [bigint] IDENTITY(1,1) NOT NULL,
	[ISIN] [nvarchar](12) COLLATE French_CI_AS NOT NULL,
	[TradingVenueMIC] [nvarchar](10) COLLATE French_CI_AS NOT NULL,
	[IssuerReqAdmission] [nvarchar](10) COLLATE French_CI_AS NULL,
	[AdmissionApprvlDate] [date] NULL,
	[ReqForAdmissionDate] [date] NULL,
	[FirstTradingDate] [date] NULL,
	[TerminationDate] [date] NULL,
	[ValidFromDate] [date] NOT NULL,
	[ValidToDate] [date] NULL,
	[IsCurrent] [bit] NOT NULL,
	[RecordSource] [nvarchar](100) COLLATE French_CI_AS NOT NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_DimInstrumentListing_SCD2] PRIMARY KEY CLUSTERED 
(
	[InstrumentListingSK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[DimInstrument_SCD2](
	[InstrumentSK] [bigint] IDENTITY(1,1) NOT NULL,
	[ISIN] [nvarchar](12) COLLATE French_CI_AS NOT NULL,
	[FullName] [nvarchar](500) COLLATE French_CI_AS NULL,
	[ShortName] [nvarchar](200) COLLATE French_CI_AS NULL,
	[CFI] [nvarchar](20) COLLATE French_CI_AS NULL,
	[CommodityDerivativeInd] [nvarchar](10) COLLATE French_CI_AS NULL,
	[NotionalCurrency] [nvarchar](3) COLLATE French_CI_AS NULL,
	[IssuerLEI] [nvarchar](20) COLLATE French_CI_AS NULL,
	[ValidFromDate] [date] NOT NULL,
	[ValidToDate] [date] NULL,
	[IsCurrent] [bit] NOT NULL,
	[RecordSource] [nvarchar](100) COLLATE French_CI_AS NOT NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
	[CmdtyBaseProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubProduct] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtyTransactionType] [varchar](50) COLLATE French_CI_AS NULL,
	[CmdtyFinalPriceType] [varchar](50) COLLATE French_CI_AS NULL,
	[TotalIssuedNominalAmount] [decimal](38, 10) NULL,
	[TotalIssuedNominalAmountCcy] [varchar](10) COLLATE French_CI_AS NULL,
	[MaturityDate] [date] NULL,
	[NominalValuePerUnit] [decimal](38, 10) NULL,
	[NominalValuePerUnitCcy] [varchar](10) COLLATE French_CI_AS NULL,
 CONSTRAINT [PK_DimInstrument_SCD2] PRIMARY KEY CLUSTERED 
(
	[InstrumentSK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[DimCurrency](
	[CurrencySK] [int] IDENTITY(1,1) NOT NULL,
	[CurrencyCode] [nvarchar](3) COLLATE French_CI_AS NOT NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_DimCurrency] PRIMARY KEY CLUSTERED 
(
	[CurrencySK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[DimCFI](
	[CFISK] [int] IDENTITY(1,1) NOT NULL,
	[CFI] [nvarchar](20) COLLATE French_CI_AS NOT NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
	[Category] [nvarchar](50) COLLATE French_CI_AS NULL,
	[Group] [nvarchar](80) COLLATE French_CI_AS NULL,
	[Type] [nvarchar](80) COLLATE French_CI_AS NULL,
	[Has_Strike] [bit] NULL,
	[Is_Derivative] [bit] NULL,
	[Exercise_Style] [nvarchar](30) COLLATE French_CI_AS NULL,
	[Underlying_Class] [nvarchar](50) COLLATE French_CI_AS NULL,
	[ESMA_Reportable] [bit] NULL,
	[CFI_1] [char](1) COLLATE French_CI_AS NULL,
	[CFI_2] [char](1) COLLATE French_CI_AS NULL,
	[CFI_3] [char](1) COLLATE French_CI_AS NULL,
	[CFI_4] [char](1) COLLATE French_CI_AS NULL,
	[CFI_5] [char](1) COLLATE French_CI_AS NULL,
	[CFI_6] [char](1) COLLATE French_CI_AS NULL,
	[CFI_Level1_Code] [char](1) COLLATE French_CI_AS NULL,
	[CFI_Level2_Code] [char](1) COLLATE French_CI_AS NULL,
	[CFI_Level3_Code] [char](1) COLLATE French_CI_AS NULL,
	[CFI_Level1_Label_FR] [nvarchar](80) COLLATE French_CI_AS NULL,
	[CFI_Resume_FR] [nvarchar](400) COLLATE French_CI_AS NULL,
	[DataConfidence] [nvarchar](20) COLLATE French_CI_AS NULL,
 CONSTRAINT [PK_DimCFI] PRIMARY KEY CLUSTERED 
(
	[CFISK] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[FactInstrumentSnapshot_Latest](
	[SnapshotDate] [date] NOT NULL,
	[CurrencySK] [int] NULL,
	[CFISK] [int] NULL,
	[IssuerSK] [int] NULL,
	[TradingVenueSK] [int] NULL,
	[InstrumentSK] [bigint] NULL,
	[InstrumentListingSK] [bigint] NULL
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [mart].[FactInstrumentSnapshot](
	[SnapshotDate] [date] NOT NULL,
	[ISIN] [nvarchar](12) COLLATE French_CI_AS NOT NULL,
	[TradingVenueMIC] [nvarchar](10) COLLATE French_CI_AS NOT NULL,
	[CurrencySK] [int] NULL,
	[CFISK] [int] NULL,
	[IssuerSK] [int] NULL,
	[TradingVenueSK] [int] NULL,
	[InstrumentSK] [bigint] NULL,
	[InstrumentListingSK] [bigint] NULL,
	[TotalIssuedNominalAmount] [decimal](38, 10) NULL,
	[NominalValuePerUnit] [decimal](38, 10) NULL,
	[FixedRate] [decimal](38, 10) NULL,
	[PriceMultiplier] [decimal](38, 10) NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_FactInstrumentSnapshot] PRIMARY KEY CLUSTERED 
(
	[SnapshotDate] ASC,
	[ISIN] ASC,
	[TradingVenueMIC] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE   PROCEDURE mart.usp_Load_DimTradingVenue
AS
BEGIN
    SET NOCOUNT ON;

    MERGE mart.DimTradingVenue AS tgt
    USING (
        SELECT DISTINCT TradingVenueMIC
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING
        WHERE TradingVenueMIC IS NOT NULL
    ) AS src
    ON tgt.TradingVenueMIC = src.TradingVenueMIC
    WHEN NOT MATCHED THEN
        INSERT (TradingVenueMIC, LoadDtmUTC)
        VALUES (src.TradingVenueMIC, SYSUTCDATETIME());
END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON

/* ============================================================
   2) PROC : mart.usp_Load_DimIssuer
      - Source: stg.ESMA_INSTRUMENT_LISTING (IssuerLEI)
      - Enrich: stg.STG_LEI_CDF_GOLDEN (LEFT JOIN sur LEI)
      - MERGE: INSERT si nouveau, UPDATE si existant
   ============================================================ */
CREATE PROCEDURE [mart].[usp_Load_DimIssuer]
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH src_issuer AS (
        SELECT DISTINCT
            eil.IssuerLEI
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING AS eil
        WHERE eil.IssuerLEI IS NOT NULL
    ),
    src_lei_ranked AS (
        SELECT
            g.LEI,
            g.LegalName,
            g.LegalAddress_FirstAddressLine,
            g.LegalAddress_City,
            g.LegalAddress_Region,
            g.LegalAddress_Country,
            g.LegalAddress_PostalCode,
            g.HeadquartersAddress_FirstAddressLine,
            g.HeadquartersAddress_City,
            g.HeadquartersAddress_Country,
            g.HeadquartersAddress_PostalCode,
            g.LegalJurisdiction,
            g.EntityCategory,
            g.EntityStatus,
            g.LegalEntityEvent_1_group_type,
            g.InitialRegistrationDate,
            g.LastUpdateDate,
            ROW_NUMBER() OVER (
                PARTITION BY g.LEI
                ORDER BY
                    g.LastUpdateDate DESC,
                    g.InitialRegistrationDate DESC
            ) AS rn
        FROM KHLWorldInvest.stg.STG_LEI_CDF_GOLDEN AS g
        WHERE g.LEI IS NOT NULL
    ),
    src AS (
        SELECT
            i.IssuerLEI,

            l.LegalName,
            l.LegalAddress_FirstAddressLine,
            l.LegalAddress_City,
            l.LegalAddress_Region,
            l.LegalAddress_Country,
            l.LegalAddress_PostalCode,
            l.HeadquartersAddress_FirstAddressLine,
            l.HeadquartersAddress_City,
            l.HeadquartersAddress_Country,
            l.HeadquartersAddress_PostalCode,
            l.LegalJurisdiction,
            l.EntityCategory,
            l.EntityStatus,
            l.LegalEntityEvent_1_group_type,
            l.InitialRegistrationDate,
            l.LastUpdateDate
        FROM src_issuer AS i
        LEFT JOIN src_lei_ranked AS l
            ON l.LEI = i.IssuerLEI
           AND l.rn = 1
    )
    MERGE mart.DimIssuer AS tgt
    USING src
        ON tgt.IssuerLEI = src.IssuerLEI

    WHEN NOT MATCHED THEN
        INSERT (
            IssuerLEI,
            LegalName,
            LegalAddress_FirstAddressLine,
            LegalAddress_City,
            LegalAddress_Region,
            LegalAddress_Country,
            LegalAddress_PostalCode,
            HeadquartersAddress_FirstAddressLine,
            HeadquartersAddress_City,
            HeadquartersAddress_Country,
            HeadquartersAddress_PostalCode,
            LegalJurisdiction,
            EntityCategory,
            EntityStatus,
            LegalEntityEvent_1_group_type,
            InitialRegistrationDate,
            LastUpdateDate,
            LoadDtmUTC
        )
        VALUES (
            src.IssuerLEI,
            src.LegalName,
            src.LegalAddress_FirstAddressLine,
            src.LegalAddress_City,
            src.LegalAddress_Region,
            src.LegalAddress_Country,
            src.LegalAddress_PostalCode,
            src.HeadquartersAddress_FirstAddressLine,
            src.HeadquartersAddress_City,
            src.HeadquartersAddress_Country,
            src.HeadquartersAddress_PostalCode,
            src.LegalJurisdiction,
            src.EntityCategory,
            src.EntityStatus,
            src.LegalEntityEvent_1_group_type,
            src.InitialRegistrationDate,
            src.LastUpdateDate,
            SYSUTCDATETIME()
        )

    WHEN MATCHED AND (
           ISNULL(tgt.LegalName, '') <> ISNULL(src.LegalName, '')
        OR ISNULL(tgt.LegalAddress_FirstAddressLine, '') <> ISNULL(src.LegalAddress_FirstAddressLine, '')
        OR ISNULL(tgt.LegalAddress_City, '') <> ISNULL(src.LegalAddress_City, '')
        OR ISNULL(tgt.LegalAddress_Region, '') <> ISNULL(src.LegalAddress_Region, '')
        OR ISNULL(tgt.LegalAddress_Country, '') <> ISNULL(src.LegalAddress_Country, '')
        OR ISNULL(tgt.LegalAddress_PostalCode, '') <> ISNULL(src.LegalAddress_PostalCode, '')
        OR ISNULL(tgt.HeadquartersAddress_FirstAddressLine, '') <> ISNULL(src.HeadquartersAddress_FirstAddressLine, '')
        OR ISNULL(tgt.HeadquartersAddress_City, '') <> ISNULL(src.HeadquartersAddress_City, '')
        OR ISNULL(tgt.HeadquartersAddress_Country, '') <> ISNULL(src.HeadquartersAddress_Country, '')
        OR ISNULL(tgt.HeadquartersAddress_PostalCode, '') <> ISNULL(src.HeadquartersAddress_PostalCode, '')
        OR ISNULL(tgt.LegalJurisdiction, '') <> ISNULL(src.LegalJurisdiction, '')
        OR ISNULL(tgt.EntityCategory, '') <> ISNULL(src.EntityCategory, '')
        OR ISNULL(tgt.EntityStatus, '') <> ISNULL(src.EntityStatus, '')
        OR ISNULL(tgt.LegalEntityEvent_1_group_type, '') <> ISNULL(src.LegalEntityEvent_1_group_type, '')
        OR ISNULL(tgt.InitialRegistrationDate, '19000101') <> ISNULL(src.InitialRegistrationDate, '19000101')
        OR ISNULL(tgt.LastUpdateDate, '19000101') <> ISNULL(src.LastUpdateDate, '19000101')
    )
    THEN UPDATE SET
        tgt.LegalName                            = src.LegalName,
        tgt.LegalAddress_FirstAddressLine        = src.LegalAddress_FirstAddressLine,
        tgt.LegalAddress_City                    = src.LegalAddress_City,
        tgt.LegalAddress_Region                  = src.LegalAddress_Region,
        tgt.LegalAddress_Country                 = src.LegalAddress_Country,
        tgt.LegalAddress_PostalCode              = src.LegalAddress_PostalCode,
        tgt.HeadquartersAddress_FirstAddressLine = src.HeadquartersAddress_FirstAddressLine,
        tgt.HeadquartersAddress_City             = src.HeadquartersAddress_City,
        tgt.HeadquartersAddress_Country          = src.HeadquartersAddress_Country,
        tgt.HeadquartersAddress_PostalCode       = src.HeadquartersAddress_PostalCode,
        tgt.LegalJurisdiction                    = src.LegalJurisdiction,
        tgt.EntityCategory                       = src.EntityCategory,
        tgt.EntityStatus                         = src.EntityStatus,
        tgt.LegalEntityEvent_1_group_type        = src.LegalEntityEvent_1_group_type,
        tgt.InitialRegistrationDate              = src.InitialRegistrationDate,
        tgt.LastUpdateDate                       = src.LastUpdateDate,
        tgt.LoadDtmUTC                           = SYSUTCDATETIME();

END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE   PROCEDURE mart.usp_Load_DimInstrumentListing_SCD2
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#SRC') IS NOT NULL DROP TABLE #SRC;

    SELECT
        ISIN,
        TradingVenueMIC,
        IssuerReqAdmission,
        AdmissionApprvlDate = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), AdmissionApprvlDate), 10)),
        ReqForAdmissionDate = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ReqForAdmissionDate), 10)),
        FirstTradingDate    = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), FirstTradingDate), 10)),
        TerminationDate     = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), TerminationDate), 10)),
        ValidFromDate       = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)),
        ValidToDate         = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidToDate), 10)),
        RecordSource        = CAST('ESMA_INSTRUMENT_LISTING' AS nvarchar(100))
    INTO #SRC
    FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING
    WHERE ISIN IS NOT NULL
      AND TradingVenueMIC IS NOT NULL
      AND LatestRecordFlag = 1
      AND TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)) IS NOT NULL;

    /* 1) Fermer la ligne courante si changement */
    UPDATE cur
    SET
        cur.ValidToDate = DATEADD(day, -1, s.ValidFromDate),
        cur.IsCurrent   = 0,
        cur.LoadDtmUTC  = SYSUTCDATETIME()
    FROM mart.DimInstrumentListing_SCD2 cur
    JOIN #SRC s
      ON s.ISIN = cur.ISIN AND s.TradingVenueMIC = cur.TradingVenueMIC
    WHERE cur.IsCurrent = 1
      AND (
            ISNULL(cur.IssuerReqAdmission,'') <> ISNULL(s.IssuerReqAdmission,'')
         OR ISNULL(cur.AdmissionApprvlDate,'19000101') <> ISNULL(s.AdmissionApprvlDate,'19000101')
         OR ISNULL(cur.ReqForAdmissionDate,'19000101') <> ISNULL(s.ReqForAdmissionDate,'19000101')
         OR ISNULL(cur.FirstTradingDate,'19000101') <> ISNULL(s.FirstTradingDate,'19000101')
         OR ISNULL(cur.TerminationDate,'19000101') <> ISNULL(s.TerminationDate,'19000101')
      );

    /* 2) Insérer une nouvelle ligne si pas de courante */
    INSERT INTO mart.DimInstrumentListing_SCD2
    (
        ISIN, TradingVenueMIC,
        IssuerReqAdmission, AdmissionApprvlDate, ReqForAdmissionDate, FirstTradingDate, TerminationDate,
        ValidFromDate, ValidToDate, IsCurrent, RecordSource, LoadDtmUTC
    )
    SELECT
        s.ISIN, s.TradingVenueMIC,
        s.IssuerReqAdmission, s.AdmissionApprvlDate, s.ReqForAdmissionDate, s.FirstTradingDate, s.TerminationDate,
        s.ValidFromDate, s.ValidToDate,
        CASE WHEN s.ValidToDate IS NULL THEN 1 ELSE 0 END,
        s.RecordSource,
        SYSUTCDATETIME()
    FROM #SRC s
    LEFT JOIN mart.DimInstrumentListing_SCD2 cur
           ON cur.ISIN = s.ISIN
          AND cur.TradingVenueMIC = s.TradingVenueMIC
          AND cur.IsCurrent = 1
    WHERE cur.InstrumentListingSK IS NULL;
END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON

CREATE PROCEDURE [mart].[usp_Load_DimInstrument_SCD2]
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#SRC') IS NOT NULL DROP TABLE #SRC;

    ;WITH S AS
    (
        SELECT
            ISIN,
            FullName,
            ShortName,
            CFI,
            CommodityDerivativeInd,
            NotionalCurrency,
            IssuerLEI,
            ValidFromDate = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)),
            ValidToDate   = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidToDate), 10)),
            RecordSource  = CAST('ESMA_INSTRUMENT_LISTING' AS nvarchar(100)),
            rn = ROW_NUMBER() OVER
                 (
                     PARTITION BY
                         ISIN,
                         TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10))
                     ORDER BY
                         TradingVenueMIC ASC  -- arbitraire mais stable : 1 ligne par ISIN/VF
                 )
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING
        WHERE ISIN IS NOT NULL
          AND LatestRecordFlag = 1
          AND TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)) IS NOT NULL
    )
    SELECT
        ISIN, FullName, ShortName, CFI, CommodityDerivativeInd, NotionalCurrency, IssuerLEI,
        ValidFromDate, ValidToDate, RecordSource
    INTO #SRC
    FROM S
    WHERE rn = 1;

    /* 1) Fermer la ligne courante si changement */
    UPDATE cur
    SET
        cur.ValidToDate = DATEADD(day, -1, s.ValidFromDate),
        cur.IsCurrent   = 0,
        cur.LoadDtmUTC  = SYSUTCDATETIME()
    FROM mart.DimInstrument_SCD2 cur
    JOIN #SRC s
      ON s.ISIN = cur.ISIN
    WHERE cur.IsCurrent = 1
      AND (
            ISNULL(cur.FullName,'') <> ISNULL(s.FullName,'')
         OR ISNULL(cur.ShortName,'') <> ISNULL(s.ShortName,'')
         OR ISNULL(cur.CFI,'') <> ISNULL(s.CFI,'')
         OR ISNULL(cur.CommodityDerivativeInd,'') <> ISNULL(s.CommodityDerivativeInd,'')
         OR ISNULL(cur.NotionalCurrency,'') <> ISNULL(s.NotionalCurrency,'')
         OR ISNULL(cur.IssuerLEI,'') <> ISNULL(s.IssuerLEI,'')
      );

    /* 2) Insérer si la ligne (ISIN,ValidFromDate) n’existe pas déjà */
    INSERT INTO mart.DimInstrument_SCD2
    (
        ISIN, FullName, ShortName, CFI, CommodityDerivativeInd, NotionalCurrency, IssuerLEI,
        ValidFromDate, ValidToDate, IsCurrent, RecordSource, LoadDtmUTC
    )
    SELECT
        s.ISIN, s.FullName, s.ShortName, s.CFI, s.CommodityDerivativeInd, s.NotionalCurrency, s.IssuerLEI,
        s.ValidFromDate,
        s.ValidToDate,
        CASE WHEN s.ValidToDate IS NULL THEN 1 ELSE 0 END,
        s.RecordSource,
        SYSUTCDATETIME()
    FROM #SRC s
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM mart.DimInstrument_SCD2 d
        WHERE d.ISIN = s.ISIN
          AND d.ValidFromDate = s.ValidFromDate
    );

    /*==========================================================
      3) UPDATE nouvelles colonnes (enrichissement)
         - Debt fields : ESMA_INSTRUMENT_DEBT
         - Cmdty fields : ESMA_INSTRUMENT_DERIVATIVE + ESMA_INSTRUMENT_LISTING
         (sans toucher au SCD2 closing/inserting)
    ==========================================================*/
    ;WITH L AS
    (
        SELECT
            ISIN = LEFT(LTRIM(RTRIM(ISIN)),12),
            ValidFromDate = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)),
            CmdtyBaseProduct,
            CmdtySubProduct,
            CmdtyTransactionType,
            CmdtyFinalPriceType,
            rn = ROW_NUMBER() OVER
                 (
                    PARTITION BY LEFT(LTRIM(RTRIM(ISIN)),12),
                                 TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10))
                    ORDER BY TradingVenueMIC ASC
                 )
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING
        WHERE ISIN IS NOT NULL
          AND LatestRecordFlag = 1
          AND TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)) IS NOT NULL
    ),
    R AS
    (
        SELECT
            ISIN = LEFT(LTRIM(RTRIM(ISIN)),12),
            ValidFromDate = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)),
            CmdtyBaseProduct,
            CmdtySubProduct,
            CmdtyTransactionType,
            CmdtyFinalPriceType,
            rn = ROW_NUMBER() OVER
                 (
                    PARTITION BY LEFT(LTRIM(RTRIM(ISIN)),12),
                                 TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10))
                    ORDER BY TradingVenueMIC ASC
                 )
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_DERIVATIVE
        WHERE ISIN IS NOT NULL
          AND LatestRecordFlag = 1
          AND TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)) IS NOT NULL
    ),
    D AS
    (
        SELECT
            ISIN = LEFT(LTRIM(RTRIM(ISIN)),12),
            ValidFromDate = TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)),
            TotalIssuedNominalAmount,
            TotalIssuedNominalAmountCcy,
            MaturityDate,
            NominalValuePerUnit,
            NominalValuePerUnitCcy,
            rn = ROW_NUMBER() OVER
                 (
                    PARTITION BY LEFT(LTRIM(RTRIM(ISIN)),12),
                                 TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10))
                    ORDER BY TradingVenueMIC ASC
                 )
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_DEBT
        WHERE ISIN IS NOT NULL
          AND LatestRecordFlag = 1
          AND TRY_CONVERT(date, LEFT(CONVERT(nvarchar(50), ValidFromDate), 10)) IS NOT NULL
    ),
    U AS
    (
        SELECT
            ISIN = COALESCE(d.ISIN, r.ISIN, l.ISIN),
            ValidFromDate = COALESCE(d.ValidFromDate, r.ValidFromDate, l.ValidFromDate),

            -- Cmdty : priorité DERIVATIVE, sinon LISTING
            CmdtyBaseProduct     = COALESCE(r.CmdtyBaseProduct,     l.CmdtyBaseProduct),
            CmdtySubProduct      = COALESCE(r.CmdtySubProduct,      l.CmdtySubProduct),
            CmdtyTransactionType = COALESCE(r.CmdtyTransactionType, l.CmdtyTransactionType),
            CmdtyFinalPriceType  = COALESCE(r.CmdtyFinalPriceType,  l.CmdtyFinalPriceType),

            -- Debt : DEBT table
            d.TotalIssuedNominalAmount,
            d.TotalIssuedNominalAmountCcy,
            d.MaturityDate,
            d.NominalValuePerUnit,
            d.NominalValuePerUnitCcy
        FROM (SELECT * FROM D WHERE rn = 1) d
        FULL JOIN (SELECT * FROM R WHERE rn = 1) r
          ON r.ISIN = d.ISIN AND r.ValidFromDate = d.ValidFromDate
        FULL JOIN (SELECT * FROM L WHERE rn = 1) l
          ON l.ISIN = COALESCE(d.ISIN,r.ISIN)
         AND l.ValidFromDate = COALESCE(d.ValidFromDate,r.ValidFromDate)
    )
    UPDATE tgt
    SET
        tgt.CmdtyBaseProduct            = u.CmdtyBaseProduct,
        tgt.CmdtySubProduct             = u.CmdtySubProduct,
        tgt.CmdtyTransactionType        = u.CmdtyTransactionType,
        tgt.CmdtyFinalPriceType         = u.CmdtyFinalPriceType,

        tgt.TotalIssuedNominalAmount    = u.TotalIssuedNominalAmount,
        tgt.TotalIssuedNominalAmountCcy = u.TotalIssuedNominalAmountCcy,
        tgt.MaturityDate                = u.MaturityDate,
        tgt.NominalValuePerUnit         = u.NominalValuePerUnit,
        tgt.NominalValuePerUnitCcy      = u.NominalValuePerUnitCcy,

        tgt.LoadDtmUTC                  = SYSUTCDATETIME()
    FROM mart.DimInstrument_SCD2 tgt
    JOIN U u
      ON u.ISIN = tgt.ISIN
     AND u.ValidFromDate = tgt.ValidFromDate
    WHERE
          (ISNULL(tgt.CmdtyBaseProduct,'') <> ISNULL(u.CmdtyBaseProduct,'')
       OR  ISNULL(tgt.CmdtySubProduct,'') <> ISNULL(u.CmdtySubProduct,'')
       OR  ISNULL(tgt.CmdtyTransactionType,'') <> ISNULL(u.CmdtyTransactionType,'')
       OR  ISNULL(tgt.CmdtyFinalPriceType,'') <> ISNULL(u.CmdtyFinalPriceType,'')
       OR  ISNULL(tgt.TotalIssuedNominalAmount,0) <> ISNULL(u.TotalIssuedNominalAmount,0)
       OR  ISNULL(tgt.TotalIssuedNominalAmountCcy,'') <> ISNULL(u.TotalIssuedNominalAmountCcy,'')
       OR  ISNULL(tgt.MaturityDate,'19000101') <> ISNULL(u.MaturityDate,'19000101')
       OR  ISNULL(tgt.NominalValuePerUnit,0) <> ISNULL(u.NominalValuePerUnit,0)
       OR  ISNULL(tgt.NominalValuePerUnitCcy,'') <> ISNULL(u.NominalValuePerUnitCcy,'')
      );

END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE   PROCEDURE mart.usp_Load_DimCurrency
AS
BEGIN
    SET NOCOUNT ON;

    MERGE mart.DimCurrency AS tgt
    USING (
        SELECT DISTINCT NotionalCurrency AS CurrencyCode
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING
        WHERE NotionalCurrency IS NOT NULL
    ) AS src
    ON tgt.CurrencyCode = src.CurrencyCode
    WHEN NOT MATCHED THEN
        INSERT (CurrencyCode, LoadDtmUTC)
        VALUES (src.CurrencyCode, SYSUTCDATETIME());
END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON

CREATE PROCEDURE [mart].[usp_Load_DimCFI]
AS
BEGIN
    SET NOCOUNT ON;

    /* =========================================================
       1) Insert des nouveaux CFI (depuis STG)
       ========================================================= */
    MERGE mart.DimCFI AS tgt
    USING (
        SELECT DISTINCT CFI
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING
        WHERE CFI IS NOT NULL
          AND LEN(CFI) >= 2
    ) AS src
    ON tgt.CFI = src.CFI
    WHEN NOT MATCHED THEN
        INSERT (CFI, LoadDtmUTC)
        VALUES (src.CFI, SYSUTCDATETIME());

    /* =========================================================
       2) Hints pour les CFI qui commencent par J
          - On dérive Underlying_Class + Derivative_Type depuis FullName
          - Agrégation par CFI : une règle robuste "majoritaire" via MAX
       ========================================================= */
    ;WITH JHints AS (
        SELECT
            e.CFI,

            Hint_Underlying_Class =
                CASE
                    WHEN MAX(CASE WHEN UPPER(e.FullName) LIKE '%FOREIGN%EXCHANGE%' OR UPPER(e.FullName) LIKE '%FOREIGN_EXCHANGE%' THEN 1 ELSE 0 END) = 1 THEN N'FX'
                    WHEN MAX(CASE WHEN UPPER(e.FullName) LIKE '%RATES%' THEN 1 ELSE 0 END) = 1 THEN N'Interest Rate'
                    WHEN MAX(CASE WHEN UPPER(e.FullName) LIKE '%EQUITY%' THEN 1 ELSE 0 END) = 1 THEN N'Equity'
                    WHEN MAX(CASE WHEN UPPER(e.FullName) LIKE '%COMMODITIES%' THEN 1 ELSE 0 END) = 1 THEN N'Commodity'
                    ELSE N'Unknown'
                END,

            Hint_Derivative_Type =
                CASE
                    WHEN MAX(CASE WHEN UPPER(e.FullName) LIKE '%OPTION%' THEN 1 ELSE 0 END) = 1 THEN N'Option'
                    WHEN MAX(CASE WHEN UPPER(e.FullName) LIKE '%FORWARD%' OR UPPER(e.FullName) LIKE '%FWD%' THEN 1 ELSE 0 END) = 1 THEN N'Forward'
                    ELSE N'Unknown'
                END
        FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING e
        WHERE e.CFI LIKE 'J%'
          AND e.FullName IS NOT NULL
        GROUP BY e.CFI
    ),

    cte AS (
        SELECT
            d.CFISK,
            d.CFI,

            Cat = UPPER(SUBSTRING(d.CFI, 1, 1)),
            Grp = UPPER(SUBSTRING(d.CFI, 2, 1)),

            A1  = CASE WHEN LEN(d.CFI) >= 3 THEN UPPER(SUBSTRING(d.CFI, 3, 1)) END,
            A2  = CASE WHEN LEN(d.CFI) >= 4 THEN UPPER(SUBSTRING(d.CFI, 4, 1)) END,
            A3  = CASE WHEN LEN(d.CFI) >= 5 THEN UPPER(SUBSTRING(d.CFI, 5, 1)) END,
            A4  = CASE WHEN LEN(d.CFI) >= 6 THEN UPPER(SUBSTRING(d.CFI, 6, 1)) END
        FROM mart.DimCFI d
        WHERE d.CFI IS NOT NULL
          AND LEN(d.CFI) >= 2
          AND (
                d.Category IS NULL OR d.[Group] IS NULL OR d.[Type] IS NULL
             OR d.Is_Derivative IS NULL OR d.Has_Strike IS NULL OR d.Exercise_Style IS NULL
             OR d.Underlying_Class IS NULL OR d.ESMA_Reportable IS NULL
          )
    )
    UPDATE d
    SET
        /* ---------- Category ---------- */
        d.Category =
            CASE c.Cat
                WHEN 'E' THEN N'Equity (actions et assimilés)'
                WHEN 'D' THEN N'Debt (instruments de dette)'
                WHEN 'C' THEN N'Collective Investment (OPC/Fonds)'
                WHEN 'R' THEN N'Entitlement / Right (droits)'
                WHEN 'O' THEN N'Listed options (options listées)'
                WHEN 'F' THEN N'Futures (contrats à terme)'
                WHEN 'S' THEN N'Swaps'
                WHEN 'H' THEN N'Hybride / Structuré (non standard / OTC)'
                WHEN 'M' THEN N'Other / Misc (souvent structuré)'
                WHEN 'J' THEN N'Dérivés (famille J - ESMA/FIRDS)'
                ELSE N'Unknown'
            END,

        /* ---------- Group ---------- */
        d.[Group] =
            CASE c.Cat
                WHEN 'E' THEN
                    CASE c.Grp
                        WHEN 'S' THEN N'Shares (actions ordinaires)'
                        WHEN 'P' THEN N'Preferred shares (actions de préférence)'
                        WHEN 'C' THEN N'Convertible shares (actions convertibles)'
                        ELSE N'Equity - Autre'
                    END
                WHEN 'D' THEN
                    CASE c.Grp
                        WHEN 'B' THEN N'Bonds (obligations)'
                        WHEN 'N' THEN N'Notes'
                        ELSE N'Debt - Autre'
                    END
                WHEN 'C' THEN
                    CASE c.Grp
                        WHEN 'I' THEN N'Fonds / parts (units)'
                        WHEN 'E' THEN N'ETF (Exchange-traded funds)'
                        ELSE N'Collective - Autre'
                    END

                WHEN 'J' THEN
                    -- Groupe dérivé du FullName (plus fiable que CFI[2] dans ton cas)
                    CASE COALESCE(j.Hint_Underlying_Class, N'Unknown')
                        WHEN N'FX' THEN N'Foreign Exchange (change)'
                        WHEN N'Interest Rate' THEN N'Taux d’intérêt (Rates)'
                        WHEN N'Equity' THEN N'Actions (Equity)'
                        WHEN N'Commodity' THEN N'Matières premières (Commodities)'
                        ELSE N'J - Autre'
                    END

                WHEN 'O' THEN N'Option - Groupe ' + c.Grp
                WHEN 'F' THEN N'Forward/Future - Groupe ' + c.Grp
                WHEN 'S' THEN N'Swap - Groupe ' + c.Grp
                WHEN 'R' THEN N'Right - Groupe ' + c.Grp
                WHEN 'H' THEN N'Hybride/Structuré - Groupe ' + c.Grp
                WHEN 'M' THEN N'Other - Groupe ' + c.Grp
                ELSE N'Unknown'
            END,

        /* ---------- Type ---------- */
        d.[Type] =
            CASE
                WHEN c.Cat = 'J' THEN
                    CASE COALESCE(j.Hint_Derivative_Type, N'Unknown')
                        WHEN N'Option' THEN N'Option (dérivé)'
                        WHEN N'Forward' THEN N'Forward (dérivé)'
                        ELSE N'Dérivé - Type inconnu'
                    END
                WHEN c.A1 IS NULL OR c.A1 = '' THEN NULL
                WHEN c.A1 = 'X' THEN N'Non applicable / non renseigné'
                ELSE N'Attr1=' + c.A1
            END,

        /* ---------- Is_Derivative ---------- */
        d.Is_Derivative =
            CASE
                WHEN c.Cat IN ('O','F','S','H','M','J') THEN 1
                ELSE 0
            END,

        /* ---------- Has_Strike ---------- */
        d.Has_Strike =
            CASE
                WHEN c.Cat = 'O' THEN 1
                WHEN c.Cat = 'J' THEN
                    CASE WHEN j.Hint_Derivative_Type = N'Option' THEN 1 ELSE 0 END
                ELSE 0
            END,

        /* ---------- Exercise_Style ---------- */
        d.Exercise_Style =
            CASE
                WHEN c.Cat IN ('O','H') THEN
                    CASE
                        WHEN c.A2 = 'E' THEN N'Européen'
                        WHEN c.A2 = 'A' THEN N'Américain'
                        ELSE NULL
                    END
                ELSE NULL
            END,

        /* ---------- Underlying_Class ---------- */
        d.Underlying_Class =
            CASE
                WHEN c.Cat = 'E' THEN N'Equity'
                WHEN c.Cat = 'D' THEN N'Debt'
                WHEN c.Cat = 'C' THEN N'Fund / Collective'
                WHEN c.Cat = 'R' THEN N'Entitlement'
                WHEN c.Cat IN ('O','F','S') THEN N'Derivative'
                WHEN c.Cat = 'H' THEN
                    CASE c.Grp
                        WHEN 'E' THEN N'Equity'
                        WHEN 'R' THEN N'Interest Rate'
                        WHEN 'C' THEN N'Commodity'
                        WHEN 'D' THEN N'Debt'
                        ELSE N'Unknown'
                    END
                WHEN c.Cat = 'J' THEN COALESCE(j.Hint_Underlying_Class, N'Unknown')
                WHEN c.Cat = 'M' THEN N'Other / Structured'
                ELSE N'Unknown'
            END,

        /* ---------- ESMA_Reportable ---------- */
        d.ESMA_Reportable =
            CASE
                WHEN c.Cat IN ('E','D','C','R','O','F','S','H','M','J') THEN 1
                ELSE 0
            END
    FROM mart.DimCFI d
    JOIN cte c
        ON c.CFISK = d.CFISK
    LEFT JOIN JHints j
        ON j.CFI = d.CFI;

END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE   PROCEDURE [mart].[usp_Refresh_FactInstrumentSnapshot_Latest]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MaxSnapshotDate date;
    SELECT @MaxSnapshotDate = MAX(SnapshotDate)
    FROM mart.FactInstrumentSnapshot;

    -- Sécurité
    IF @MaxSnapshotDate IS NULL
        RETURN;

    -- Recharge complète de la table Latest
    DELETE FROM mart.FactInstrumentSnapshot_Latest;

    INSERT INTO mart.FactInstrumentSnapshot_Latest WITH (TABLOCK)
    (
        SnapshotDate,  CurrencySK, CFISK, IssuerSK, TradingVenueSK, InstrumentSK, InstrumentListingSK
       

    )
    SELECT
        SnapshotDate, CurrencySK, CFISK, IssuerSK, TradingVenueSK, InstrumentSK, InstrumentListingSK
              
    FROM mart.FactInstrumentSnapshot
    WHERE SnapshotDate = @MaxSnapshotDate;

    -- Recommandé : stats à jour
    UPDATE STATISTICS mart.FactInstrumentSnapshot_Latest WITH FULLSCAN;
END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE   PROCEDURE [mart].[usp_Load_FactInstrumentSnapshot]
    @SnapshotDate date = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @SnapshotDate IS NULL
        SET @SnapshotDate = CONVERT(date, SYSUTCDATETIME());
	
    -- Idempotent : on recharge la journée
    DELETE FROM mart.FactInstrumentSnapshot
    WHERE SnapshotDate = @SnapshotDate;

    INSERT INTO mart.FactInstrumentSnapshot
    (
        SnapshotDate, ISIN, TradingVenueMIC,
        CurrencySK, CFISK, IssuerSK, TradingVenueSK, InstrumentSK, InstrumentListingSK,
        TotalIssuedNominalAmount, NominalValuePerUnit, FixedRate, PriceMultiplier,
        LoadDtmUTC
    )
	select SnapshotDate, ISIN, TradingVenueMIC,
        CurrencySK, CFISK, IssuerSK, TradingVenueSK, InstrumentSK, InstrumentListingSK,
        TotalIssuedNominalAmount, NominalValuePerUnit, FixedRate, PriceMultiplier,
        LoadDtmUTC
		from
		(
    SELECT
	row_number() over (partition by  l.ISIN, l.TradingVenueMIC order by InstrumentSK desc) as rnk,
        @SnapshotDate as SnapshotDate,
        l.ISIN,
        l.TradingVenueMIC,

        cur.CurrencySK,
        cfi.CFISK,
        iss.IssuerSK,
        tv.TradingVenueSK,
        di.InstrumentSK,
        dl.InstrumentListingSK,

        TRY_CONVERT(decimal(38,10), d.TotalIssuedNominalAmount) TotalIssuedNominalAmount,
        TRY_CONVERT(decimal(38,10), d.NominalValuePerUnit) NominalValuePerUnit,
        TRY_CONVERT(decimal(38,10), d.FixedRate) FixedRate,
        TRY_CONVERT(decimal(38,10), drv.PriceMultiplier) PriceMultiplier,

        SYSUTCDATETIME() LoadDtmUTC
    FROM KHLWorldInvest.stg.ESMA_INSTRUMENT_LISTING l
    LEFT JOIN KHLWorldInvest.stg.ESMA_INSTRUMENT_DEBT d
           ON d.ISIN = l.ISIN AND d.TradingVenueMIC = l.TradingVenueMIC AND d.LatestRecordFlag = 1
    LEFT JOIN KHLWorldInvest.stg.ESMA_INSTRUMENT_DERIVATIVE drv
           ON drv.ISIN = l.ISIN AND drv.TradingVenueMIC = l.TradingVenueMIC AND drv.LatestRecordFlag = 1

    LEFT JOIN mart.DimCurrency cur ON cur.CurrencyCode = l.NotionalCurrency
    LEFT JOIN mart.DimCFI cfi      ON cfi.CFI = l.CFI
    LEFT JOIN mart.DimIssuer iss   ON iss.IssuerLEI = l.IssuerLEI
    LEFT JOIN mart.DimTradingVenue tv ON tv.TradingVenueMIC = l.TradingVenueMIC

    LEFT JOIN mart.DimInstrument_SCD2 di
           ON di.ISIN = l.ISIN AND di.IsCurrent = 1
    LEFT JOIN mart.DimInstrumentListing_SCD2 dl
           ON dl.ISIN = l.ISIN AND dl.TradingVenueMIC = l.TradingVenueMIC AND dl.IsCurrent = 1

    WHERE l.ISIN IS NOT NULL
      AND l.TradingVenueMIC IS NOT NULL
      AND l.LatestRecordFlag = 1) a
	  where rnk=1
END;

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF

CREATE PROCEDURE [mart].[usp_Run_Daily_Mart_Load]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LaunchTs datetime2(0) = SYSDATETIME();
    DECLARE @ScriptName nvarchar(255) = N'mart.usp_Run_Daily_Mart_Load';

    DECLARE @StartStep datetime2(0);
    DECLARE @EndStep datetime2(0);
    DECLARE @rc_before bigint, @rc_after bigint;

    DECLARE @mindate_to_delete datetime = null;

    BEGIN TRY
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@LaunchTs,N'START',N'RUN',CONCAT(N'mindate_to_delete=',CONVERT(nvarchar(30),@mindate_to_delete,126)));

        -------------------------------------------------------------------------
        -- DimCFI
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimCFI') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_DimCFI (before)',N'mart.DimCFI',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_DimCFI;

        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimCFI') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_DimCFI (after)',N'mart.DimCFI',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));

        -------------------------------------------------------------------------
        -- DimCurrency
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimCurrency') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_DimCurrency (before)',N'mart.DimCurrency',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_DimCurrency;

        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimCurrency') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_DimCurrency (after)',N'mart.DimCurrency',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));

        -------------------------------------------------------------------------
        -- DimIssuer
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimIssuer') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_DimIssuer (before)',N'mart.DimIssuer',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_DimIssuer;

        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimIssuer') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_DimIssuer (after)',N'mart.DimIssuer',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));

        -------------------------------------------------------------------------
        -- DimTradingVenue
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimTradingVenue') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_DimTradingVenue (before)',N'mart.DimTradingVenue',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_DimTradingVenue;

        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimTradingVenue') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_DimTradingVenue (after)',N'mart.DimTradingVenue',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));

        -------------------------------------------------------------------------
        -- DimInstrument_SCD2
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimInstrument_SCD2') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_DimInstrument_SCD2 (before)',N'mart.DimInstrument_SCD2',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_DimInstrument_SCD2;

        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimInstrument_SCD2') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_DimInstrument_SCD2 (after)',N'mart.DimInstrument_SCD2',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));

        -------------------------------------------------------------------------
        -- DimInstrumentListing_SCD2
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimInstrumentListing_SCD2') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_DimInstrumentListing_SCD2 (before)',N'mart.DimInstrumentListing_SCD2',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_DimInstrumentListing_SCD2;

        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.DimInstrumentListing_SCD2') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_DimInstrumentListing_SCD2 (after)',N'mart.DimInstrumentListing_SCD2',
                CONCAT(N'rowcount_after=',@rc_after, N'; delta=',(@rc_after-@rc_before)));

        -------------------------------------------------------------------------
        -- FactInstrumentSnapshot
        -------------------------------------------------------------------------
        SET @StartStep = SYSDATETIME();
        SELECT @rc_before = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.FactInstrumentSnapshot') AND index_id IN (0,1);

        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,N'STEP: EXEC mart.usp_Load_FactInstrumentSnapshot (before)',N'mart.FactInstrumentSnapshot',CONCAT(N'rowcount_before=',@rc_before));

        EXEC mart.usp_Load_FactInstrumentSnapshot @mindate_to_delete;
        EXEC mart.usp_Refresh_FactInstrumentSnapshot_Latest;
        
        SELECT @rc_after = COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID(N'mart.FactInstrumentSnapshot') AND index_id IN (0,1);

        SET @EndStep = SYSDATETIME();
        INSERT INTO [AUDIT_BI].[log].[ESMA_Load_Log] ([ScriptName],[LaunchTimestamp],[StartTime],[EndTime],[Message],[Element],[Complement])
        VALUES (@ScriptName,@LaunchTs,@StartStep,@EndStep,N'STEP: EXEC mart.usp_Load_FactInstrumentSnapshot (after)',N'mart.FactInstrumentSnapshot',
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

