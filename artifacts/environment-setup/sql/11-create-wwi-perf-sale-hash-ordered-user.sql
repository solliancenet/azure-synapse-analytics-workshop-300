EXECUTE AS USER = 'asa.sql.highperf'

IF OBJECT_ID(N'[wwi_perf].[Sale_Hash_Ordered_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_perf].[Sale_Hash_Ordered_#USER_CONTEXT#] 

CREATE TABLE [wwi_perf].[Sale_Hash_Ordered_#USER_CONTEXT#]
WITH
(
    DISTRIBUTION = HASH ( [CustomerId] ),
    CLUSTERED COLUMNSTORE INDEX ORDER( [CustomerId] )
)
AS
SELECT
    *
FROM	
    [wwi_perf].[Sale_Heap_#USER_CONTEXT#]
OPTION  (LABEL  = 'CTAS : Sale_Hash_Ordered_#USER_CONTEXT#', MAXDOP 1)