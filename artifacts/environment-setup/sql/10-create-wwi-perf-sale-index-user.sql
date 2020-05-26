EXECUTE AS USER = 'asa.sql.highperf'

IF OBJECT_ID(N'[wwi_perf].[Sale_Index_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_perf].[Sale_Index_#USER_CONTEXT#] 

CREATE TABLE [wwi_perf].[Sale_Index_#USER_CONTEXT#]
WITH
(
	DISTRIBUTION = HASH ( [CustomerId] ),
	CLUSTERED INDEX (CustomerId)
)
AS
SELECT
	*
FROM	
	[wwi_perf].[Sale_Heap_#USER_CONTEXT#]
OPTION  (LABEL  = 'CTAS : Sale_Index_#USER_CONTEXT#')
