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
CREATE TABLE [mart].[DimCountry](
	[CountrySK] [int] IDENTITY(1,1) NOT NULL,
	[CountryCode] [char](2) COLLATE French_CI_AS NOT NULL,
	[CountryName] [nvarchar](100) COLLATE French_CI_AS NOT NULL,
	[FlagURL] [nvarchar](200) COLLATE French_CI_AS NULL,
	[LoadDtmUTC] [datetime2](0) NOT NULL,
	[Region] [nvarchar](50) COLLATE French_CI_AS NOT NULL,
 CONSTRAINT [PK_DimCountry] PRIMARY KEY CLUSTERED 
(
	[CountrySK] ASC
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
CREATE TABLE [mart].[ISO10383_MIC](
	[MIC] [nvarchar](255) COLLATE French_CI_AS NULL,
	[OPERATING MIC] [nvarchar](255) COLLATE French_CI_AS NULL,
	[OPRT/SGMT] [nvarchar](255) COLLATE French_CI_AS NULL,
	[MARKET NAME-INSTITUTION DESCRIPTION] [nvarchar](255) COLLATE French_CI_AS NULL,
	[LEGAL ENTITY NAME] [nvarchar](255) COLLATE French_CI_AS NULL,
	[LEI] [nvarchar](255) COLLATE French_CI_AS NULL,
	[MARKET CATEGORY CODE] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ACRONYM] [nvarchar](255) COLLATE French_CI_AS NULL,
	[ISO COUNTRY CODE (ISO 3166)] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CITY] [nvarchar](255) COLLATE French_CI_AS NULL,
	[WEBSITE] [nvarchar](255) COLLATE French_CI_AS NULL,
	[STATUS] [nvarchar](255) COLLATE French_CI_AS NULL,
	[CREATION DATE] [nvarchar](255) COLLATE French_CI_AS NULL,
	[LAST UPDATE DATE] [nvarchar](255) COLLATE French_CI_AS NULL,
	[LAST VALIDATION DATE] [nvarchar](255) COLLATE French_CI_AS NULL,
	[EXPIRY DATE] [nvarchar](255) COLLATE French_CI_AS NULL,
	[COMMENTS] [nvarchar](255) COLLATE French_CI_AS NULL
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

