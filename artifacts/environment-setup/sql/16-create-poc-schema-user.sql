IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'wwi_poc')
BEGIN
EXEC('CREATE SCHEMA wwi_poc')
END

IF OBJECT_ID(N'[wwi_poc].[Customer_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_poc].[Customer_#USER_CONTEXT#]  

CREATE TABLE wwi_poc.Customer_#USER_CONTEXT#
(
    CustomerId INT NOT NULL
    ,FirstName NVARCHAR(1000) NOT NULL
    ,MiddleInitial NVARCHAR(10) NULL
    ,LastName NVARCHAR(1000) NOT NULL
    ,FullName NVARCHAR(2010) NOT NULL
    ,Gender NVARCHAR(100) NULL
    ,Age INT NULL
    ,BirthDate DATE NULL
    ,Address_PostalCode NVARCHAR(200) NULL
    ,Address_Street NVARCHAR(2000) NULL
    ,Address_City NVARCHAR(2000) NULL
    ,Address_Country NVARCHAR(2000) NULL
    ,Mobile NVARCHAR(500) NULL
    ,Email NVARCHAR(500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)

IF OBJECT_ID(N'[wwi_poc].[Date_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_poc].[Date_#USER_CONTEXT#]  

CREATE TABLE [wwi_poc].[Date_#USER_CONTEXT#]
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
AS
SELECT
	*
FROM	
	[wwi].[Date]



IF OBJECT_ID(N'[wwi_poc].[Product_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_poc].[Product_#USER_CONTEXT#]  

CREATE TABLE [wwi_poc].[Product_#USER_CONTEXT#]
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
AS
SELECT
	*
FROM	
	[wwi].[Product]