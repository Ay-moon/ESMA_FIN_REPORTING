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
CREATE TABLE [dbo].[AuditLog_SystemHealth](
	[CheckID] [int] IDENTITY(1,1) NOT NULL,
	[CheckDate] [datetime2](7) NULL,
	[SourceDatabase] [nvarchar](128) COLLATE French_CI_AS NULL,
	[DatabaseSize_MB] [decimal](12, 2) NULL,
	[FragmentationPercent] [decimal](5, 2) NULL,
	[IndexCount] [int] NULL,
	[HighFragmentation_Count] [int] NULL,
	[TableCount] [int] NULL,
	[BufferPool_MB] [decimal](10, 2) NULL,
	[BufferPool_Percent] [decimal](5, 2) NULL,
	[Status] [nvarchar](50) COLLATE French_CI_AS NULL,
	[Details] [nvarchar](max) COLLATE French_CI_AS NULL,
	[CreatedAt] [datetime2](7) NULL,
PRIMARY KEY CLUSTERED 
(
	[CheckID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

