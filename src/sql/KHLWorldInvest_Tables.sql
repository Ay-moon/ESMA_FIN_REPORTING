SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[STG_LEI_REPORTING_EXCEPTION](
	[ScriptName] [nvarchar](200) COLLATE French_CI_AS NOT NULL,
	[LaunchTimestamp] [datetime2](0) NOT NULL,
	[SourceFileName] [nvarchar](260) COLLATE French_CI_AS NOT NULL,
	[SourceFileTimestamp] [datetime2](0) NOT NULL,
	[SourceUrl] [nvarchar](1000) COLLATE French_CI_AS NULL,
	[LEI] [char](20) COLLATE French_CI_AS NOT NULL,
	[Category] [nvarchar](120) COLLATE French_CI_AS NOT NULL,
	[Reason_1] [nvarchar](120) COLLATE French_CI_AS NULL,
	[Reason_2] [nvarchar](120) COLLATE French_CI_AS NULL,
	[Reason_3] [nvarchar](120) COLLATE French_CI_AS NULL,
	[Reference_1] [nvarchar](500) COLLATE French_CI_AS NULL,
	[Reference_2] [nvarchar](500) COLLATE French_CI_AS NULL,
	[Reference_3] [nvarchar](500) COLLATE French_CI_AS NULL
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[STG_LEI_RELATION](
	[StartNode_NodeID] [char](20) COLLATE French_CI_AS NULL,
	[StartNode_NodeIDType] [nvarchar](50) COLLATE French_CI_AS NULL,
	[EndNode_NodeID] [char](20) COLLATE French_CI_AS NULL,
	[EndNode_NodeIDType] [nvarchar](50) COLLATE French_CI_AS NULL,
	[RelationshipType] [nvarchar](80) COLLATE French_CI_AS NULL,
	[RelationshipStatus] [nvarchar](50) COLLATE French_CI_AS NULL,
	[Period_1_startDate] [datetime2](0) NULL,
	[Period_1_endDate] [datetime2](0) NULL,
	[Period_1_periodType] [nvarchar](50) COLLATE French_CI_AS NULL,
	[Qualifiers_1_QualifierDimension] [nvarchar](80) COLLATE French_CI_AS NULL,
	[Qualifiers_1_QualifierCategory] [nvarchar](80) COLLATE French_CI_AS NULL,
	[Quantifiers_1_MeasurementMethod] [nvarchar](80) COLLATE French_CI_AS NULL,
	[Quantifiers_1_QuantifierAmount] [decimal](20, 6) NULL,
	[Quantifiers_1_QuantifierUnits] [nvarchar](50) COLLATE French_CI_AS NULL,
	[InitialRegistrationDate] [datetime2](0) NULL,
	[LastUpdateDate] [datetime2](0) NULL,
	[RegistrationStatus] [nvarchar](50) COLLATE French_CI_AS NULL,
	[NextRenewalDate] [datetime2](0) NULL,
	[ManagingLOU] [char](20) COLLATE French_CI_AS NULL,
	[ValidationSources] [nvarchar](100) COLLATE French_CI_AS NULL,
	[ValidationDocuments] [nvarchar](500) COLLATE French_CI_AS NULL,
	[ValidationReference] [nvarchar](500) COLLATE French_CI_AS NULL
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
CREATE TABLE [stg].[ESMA_FULINS_WIDE](
	[HeaderReportingMarketId] [nvarchar](255) COLLATE French_CI_AS NULL,
	[HeaderReportingNCA] [nvarchar](255) COLLATE French_CI_AS NULL,
	[HeaderReportingPeriodDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[SourceFileName] [nvarchar](255) COLLATE French_CI_AS NULL,
	[TechRcrdId] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ISIN] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FullName] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ShortName] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CFI] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CommodityDerivativeInd] [nvarchar](255) COLLATE French_CI_AS NULL,
	[NotionalCurrency] [nvarchar](255) COLLATE French_CI_AS NULL,
	[IssuerLEI] [nvarchar](255) COLLATE French_CI_AS NULL,
	[TradingVenueMIC] [nvarchar](255) COLLATE French_CI_AS NULL,
	[IssuerReqAdmission] [nvarchar](255) COLLATE French_CI_AS NULL,
	[AdmissionApprvlDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ReqForAdmissionDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FirstTradingDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[TerminationDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[TotalIssuedNominalAmount] [nvarchar](255) COLLATE French_CI_AS NULL,
	[TotalIssuedNominalAmountCcy] [nvarchar](255) COLLATE French_CI_AS NULL,
	[MaturityDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[NominalValuePerUnit] [nvarchar](255) COLLATE French_CI_AS NULL,
	[NominalValuePerUnitCcy] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FixedRate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FloatRefRateISIN] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FloatRefRateIndex] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FloatTermUnit] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FloatTermValue] [nvarchar](255) COLLATE French_CI_AS NULL,
	[FloatBasisPointSpread] [nvarchar](255) COLLATE French_CI_AS NULL,
	[DebtSeniority] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ExpiryDate] [nvarchar](255) COLLATE French_CI_AS NULL,
	[PriceMultiplier] [nvarchar](255) COLLATE French_CI_AS NULL,
	[UnderlyingISIN] [nvarchar](255) COLLATE French_CI_AS NULL,
	[UnderlyingLEI] [nvarchar](255) COLLATE French_CI_AS NULL,
	[UnderlyingIndexRef] [nvarchar](255) COLLATE French_CI_AS NULL,
	[UnderlyingIndexTermUnit] [nvarchar](255) COLLATE French_CI_AS NULL,
	[UnderlyingIndexTermValue] [nvarchar](255) COLLATE French_CI_AS NULL,
	[OptionType] [nvarchar](255) COLLATE French_CI_AS NULL,
	[OptionExerciseStyle] [nvarchar](255) COLLATE French_CI_AS NULL,
	[DeliveryType] [nvarchar](255) COLLATE French_CI_AS NULL,
	[StrikePrice] [nvarchar](255) COLLATE French_CI_AS NULL,
	[StrikePriceCcy] [nvarchar](255) COLLATE French_CI_AS NULL,
	[StrikeNoPriceCcy] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CmdtyBaseProduct] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CmdtySubProduct] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CmdtySubSubProduct] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CmdtyTransactionType] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CmdtyFinalPriceType] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ValidFromDate] [date] NULL,
	[ValidToDate] [date] NULL,
	[LatestRecordFlag] [bit] NULL
) ON [PRIMARY]

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [stg].[ESMA_DLTINS_WIDE](
	[HeaderReportingMarketId] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeaderReportingNCA] [nvarchar](50) COLLATE French_CI_AS NULL,
	[HeaderReportingPeriodDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[SourceFileName] [nvarchar](260) COLLATE French_CI_AS NULL,
	[TechRcrdId] [nvarchar](64) COLLATE French_CI_AS NULL,
	[ISIN] [nvarchar](12) COLLATE French_CI_AS NULL,
	[FullName] [nvarchar](500) COLLATE French_CI_AS NULL,
	[ShortName] [nvarchar](200) COLLATE French_CI_AS NULL,
	[CFI] [nvarchar](20) COLLATE French_CI_AS NULL,
	[CommodityDerivativeInd] [nvarchar](10) COLLATE French_CI_AS NULL,
	[NotionalCurrency] [nvarchar](3) COLLATE French_CI_AS NULL,
	[IssuerLEI] [nvarchar](20) COLLATE French_CI_AS NULL,
	[TradingVenueMIC] [nvarchar](10) COLLATE French_CI_AS NULL,
	[IssuerReqAdmission] [nvarchar](10) COLLATE French_CI_AS NULL,
	[AdmissionApprvlDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[ReqForAdmissionDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[FirstTradingDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[TerminationDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[TotalIssuedNominalAmount] [nvarchar](50) COLLATE French_CI_AS NULL,
	[TotalIssuedNominalAmountCcy] [nvarchar](10) COLLATE French_CI_AS NULL,
	[MaturityDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[NominalValuePerUnit] [nvarchar](50) COLLATE French_CI_AS NULL,
	[NominalValuePerUnitCcy] [nvarchar](10) COLLATE French_CI_AS NULL,
	[FixedRate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[FloatRefRateISIN] [nvarchar](50) COLLATE French_CI_AS NULL,
	[FloatRefRateIndex] [nvarchar](50) COLLATE French_CI_AS NULL,
	[FloatTermUnit] [nvarchar](10) COLLATE French_CI_AS NULL,
	[FloatTermValue] [nvarchar](50) COLLATE French_CI_AS NULL,
	[FloatBasisPointSpread] [nvarchar](50) COLLATE French_CI_AS NULL,
	[DebtSeniority] [nvarchar](20) COLLATE French_CI_AS NULL,
	[ExpiryDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[PriceMultiplier] [nvarchar](50) COLLATE French_CI_AS NULL,
	[UnderlyingISIN] [nvarchar](50) COLLATE French_CI_AS NULL,
	[UnderlyingLEI] [nvarchar](50) COLLATE French_CI_AS NULL,
	[UnderlyingIndexRef] [nvarchar](200) COLLATE French_CI_AS NULL,
	[UnderlyingIndexTermUnit] [nvarchar](10) COLLATE French_CI_AS NULL,
	[UnderlyingIndexTermValue] [nvarchar](50) COLLATE French_CI_AS NULL,
	[OptionType] [nvarchar](20) COLLATE French_CI_AS NULL,
	[OptionExerciseStyle] [nvarchar](20) COLLATE French_CI_AS NULL,
	[DeliveryType] [nvarchar](20) COLLATE French_CI_AS NULL,
	[StrikePrice] [nvarchar](50) COLLATE French_CI_AS NULL,
	[StrikePriceCcy] [nvarchar](10) COLLATE French_CI_AS NULL,
	[StrikeNoPriceCcy] [nvarchar](10) COLLATE French_CI_AS NULL,
	[CmdtyBaseProduct] [nvarchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubProduct] [nvarchar](50) COLLATE French_CI_AS NULL,
	[CmdtySubSubProduct] [nvarchar](50) COLLATE French_CI_AS NULL,
	[CmdtyTransactionType] [nvarchar](50) COLLATE French_CI_AS NULL,
	[CmdtyFinalPriceType] [nvarchar](50) COLLATE French_CI_AS NULL,
	[ValidFromDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[ValidToDate] [nvarchar](50) COLLATE French_CI_AS NULL,
	[LatestRecordFlag] [bit] NULL,
	[ActionType] [varchar](10) COLLATE French_CI_AS NOT NULL
) ON [PRIMARY]

ALTER TABLE [log].[ESMA_Load_Log] ADD  CONSTRAINT [DF_ESMA_Load_Log_CreatedOn]  DEFAULT (sysdatetime()) FOR [CreatedOn]
ALTER TABLE [stg].[ESMA_INSTRUMENT_LISTING] ADD  CONSTRAINT [DF_stg_ESMA_INSTRUMENT_LISTING_ValidFromDatePK]  DEFAULT (CONVERT([date],'19000101')) FOR [ValidFromDate_PK]
ALTER TABLE [stg].[ESMA_INSTRUMENT_LISTING] ADD  CONSTRAINT [DF_stg_ESMA_INSTRUMENT_LISTING_LoadDtmUTC]  DEFAULT (sysutcdatetime()) FOR [LoadDtmUTC]
ALTER TABLE [stg].[ESMA_INSTRUMENT_DERIVATIVE] ADD  CONSTRAINT [DF_stg_ESMA_INSTRUMENT_DERIV_ValidFromDatePK]  DEFAULT (CONVERT([date],'19000101')) FOR [ValidFromDate_PK]
ALTER TABLE [stg].[ESMA_INSTRUMENT_DERIVATIVE] ADD  CONSTRAINT [DF_stg_ESMA_INSTRUMENT_DERIV_LoadDtmUTC]  DEFAULT (sysutcdatetime()) FOR [LoadDtmUTC]
ALTER TABLE [stg].[ESMA_INSTRUMENT_DEBT] ADD  CONSTRAINT [DF_stg_ESMA_INSTRUMENT_DEBT_ValidFromDatePK]  DEFAULT (CONVERT([date],'19000101')) FOR [ValidFromDate_PK]
ALTER TABLE [stg].[ESMA_INSTRUMENT_DEBT] ADD  CONSTRAINT [DF_stg_ESMA_INSTRUMENT_DEBT_LoadDtmUTC]  DEFAULT (sysutcdatetime()) FOR [LoadDtmUTC]
