
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE Name = 'CEO')
create user [CEO] without login
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE Name = 'DataAnalystMiami')
create user [DataAnalystMiami] without login
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE Name = 'DataAnalystSanDiego')
create user [DataAnalystSanDiego] without login
GO

IF OBJECT_ID(N'[wwi_security].[CustomerInfo_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_security].[CustomerInfo_#USER_CONTEXT#]

CREATE TABLE [wwi_security].[CustomerInfo_#USER_CONTEXT#]
( 
	[UserName] [nvarchar](100)  NULL,
	[Gender] [nvarchar](10)  NULL,
	[Phone] [nvarchar](50)  NULL,
	[Email] [nvarchar](150)  NULL,
	[CreditCard] [nvarchar](21)  NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
)
GO

COPY INTO [wwi_security].[CustomerInfo_#USER_CONTEXT#]
FROM 'https://#DATA_LAKE_ACCOUNT_NAME#.dfs.core.windows.net/wwi-02/security/customerinfo.csv'
WITH (
	CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '#DATA_LAKE_ACCOUNT_KEY#'),
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
	FIELDQUOTE='''',
    ENCODING='UTF8'
)
GO

IF OBJECT_ID(N'[wwi_security].[Sale_#USER_CONTEXT#]', N'U') IS NOT NULL  
DROP TABLE [wwi_security].[Sale_#USER_CONTEXT#]
GO

CREATE TABLE [wwi_security].[Sale_#USER_CONTEXT#]
( 
	[ProductId] [int]  NOT NULL,
	[Analyst] [nvarchar](100)  NOT NULL,
	[Product] [nvarchar](200)  NOT NULL,
	[CampaignName] [nvarchar](200)  NOT NULL,
	[Quantity] [int]  NOT NULL,
	[Region] [nvarchar](50)  NOT NULL,
	[State] [nvarchar](50)  NOT NULL,
	[City] [nvarchar](50)  NOT NULL,
	[Revenue] [nvarchar](50)  NULL,
	[RevenueTarget] [nvarchar](50)  NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

COPY INTO [wwi_security].[Sale_#USER_CONTEXT#]
FROM 'https://#DATA_LAKE_ACCOUNT_NAME#.dfs.core.windows.net/wwi-02/security/factsale.csv'
WITH (
	CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '#DATA_LAKE_ACCOUNT_KEY#'),
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
	FIELDQUOTE='''',
    ENCODING='UTF8'
)
GO
