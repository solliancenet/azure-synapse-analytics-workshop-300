# Activity 03: Data Warehouse Optimization - Solution

1. Check the number of records in the table (where `SUFFIX` is your **student ID**):

```sql
SELECT COUNT(*) FROM [wwi_perf].[Sale_Heap_SUFFIX]
```

2. Check the structure of the existing table:

```sql
CREATE TABLE [wwi_perf].[Sale_Heap_SUFFIX]
( 
	[TransactionId] [uniqueidentifier]  NOT NULL,
	[CustomerId] [int]  NOT NULL,
	[ProductId] [smallint]  NOT NULL,
	[Quantity] [tinyint]  NOT NULL,
	[Price] [decimal](9,2)  NOT NULL,
	[TotalAmount] [decimal](9,2)  NOT NULL,
	[TransactionDateId] [int]  NOT NULL,
	[ProfitAmount] [decimal](9,2)  NOT NULL,
	[Hour] [tinyint]  NOT NULL,
	[Minute] [tinyint]  NOT NULL,
	[StoreId] [smallint]  NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
```

3. Check the range of `TransactionDateId`:

```sql
SELECT MIN(TransactionDateId), MAX(TransactionDateId) from [wwi_perf].[Sale_Heap_SUFFIX]
```

4. Create the optimized table with `CustomerID`-based hash distribution, Clustered Columnstore Index, and four partitions:

```sql
	
CREATE TABLE [wwi_perf].[Sale_184865_Solution]
WITH
(
	DISTRIBUTION = HASH ( [CustomerId] ),
	CLUSTERED COLUMNSTORE INDEX,
	PARTITION
	(
		[TransactionDateId] RANGE RIGHT FOR VALUES (
            20190101, 20190401, 20190701, 20191001)
	)
)
AS
SELECT
	*
FROM	
	[wwi_perf].[Sale_Heap_184865]
OPTION  (LABEL  = 'CTAS : Sale_184865_Solution')
```