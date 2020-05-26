EXECUTE AS USER = 'asa.sql.highperf'

IF OBJECT_ID(N'[wwi_perf].[Sale_Heap_#USER_CONTEXT#]', N'U') IS NOT NULL   
DROP TABLE [wwi_perf].[Sale_Heap_#USER_CONTEXT#]  

CREATE TABLE [wwi_perf].[Sale_Heap_#USER_CONTEXT#]
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
AS
SELECT
	*
FROM	
	[wwi].[SaleSmall]
WHERE
	TransactionDateId >= 20190101
OPTION  (LABEL  = 'CTAS : Sale_Heap_#USER_CONTEXT#')