# Coach guidance

## What does a coach do

### Teams

* Facilitate collaboration​
* Set expectations​
* Ask leading questions, offer resources instead of answers​
* Encourage creativity​
* Problem solve: technical and interpersonal

### Manage sentiment

* Content will be challenging and **frustrating** at times​
* Let them be challenged but not blocked​
* Check in frequently and raise any issues or what is working well

## Some things to keep in mind

* No step-by-step instructions or "right" answer​
* Not all pain points are removed​
* Everyone should be learning and having fun. That includes YOU!​
* Keep Challenge Content and reference solutions private​

> Teams may not complete all challenges. However, if they are unable to import all data in the first challenge, they can safely move on to the remaining challenges.

## 1 - Configure the environment and raw import

### Happy path

The team starts out by exploring the source data **and** the existing data that's already been moved into the SQL pool tables. They may assume that the tables are already good to go, but they're not.

There are various issues with the source files, as outlined in the table below, under *things to watch out for*. Because of these issues, it will take varying levels of skill from the team members to overcome them and successfully complete this and the challenge that follows. Ideally, the team will divide and conquer this challenge by having some focus on the Parquet files and others focus on the CSV files, as each have their unique set of problems.

### Coach's notes

Have attendees show you the following before you sign off on this challenge:

* All source files (Parquet and CSV) have been successfully imported into the SQL pool.

If you see the team try to initially import all the data at once, suggest to them that they should experiment with a subset of the data beforehand. That way, if they discover any unforeseen issues, they will not have wasted valuable time.

If the team is completely stuck and have spent more than 2 hours on this challenge, move them along to the third challenge, which is about creating new queries and reports. Loading the remaining data is not a prerequisite for those challenges.

### Things to watch out for

There are some serious challenges with the source data, including:

| Name | Description |
| --- | --- |
| Poor initial table design | There is already some data in the SQL pool in several poorly designed fact tables. The following problems are "hidden" in the structure: <br>- Sub-optimal distribution strategy<br>- Sub-optimal partitioning strategy<br>- Incorrect indexing<br>- Incorrect data types used for several fields<br><br> The purpose is to mislead attendees in (wrongly) assuming the existing data is "good to go". When this assumption is made without corrective actions, all the subsequent tasks will be impacted. |
| Missing CR-LF in several CSV files | Some of the external CSV files are "corrupted". A misbehaving export tool has removed all CR-LF characters, literally leaving the files as huge, one-row files.<br><br>The purpose is to force advanced, high-scale data exploration and preparation. Should only work in a decent amount of time if Spark is used. They will need to realize it's not possible in the SQL world and will need to move into Spark.<br><br>The second CSV file is messed up in a sense that it's 140 MB one liner (the row delimiter has been replaced with `,`). |


If the team does not take the time to evaluate the data before immediately importing it, they will fail. There are various skill levels involved in extracting, transforming, and loading this data.

* You should look in the wwi_poc.Sale/Customer/Product/Data tables for existing data in the SQL Pool.
* Customer data is only partially imported. Issues with the processing of customer information prevented a complete import of customer data.

### Possible solution for the mal-formatted CSV file:

```python
from azure.storage.blob import BlockBlobService
block_blob_service = BlockBlobService(
    account_name='asadatalakeNNNNNN', account_key='...')
file_content = block_blob_service.get_blob_to_text('wwi-02', 'sale-poc/sale-20170502.csv')

# Experiment tokenizing until you figure out there's , all over the place:

tokens = file_content.content.split(',')
print(f'Found {len(tokens)} tokens in content.')

# Start looking for repeating patterns:
[tokens[i] for i in np.arange(0, 10)]
[tokens[i] for i in np.arange(0, 15)]
[tokens[i] for i in np.arange(0, 20)]

# Build an array of arrays:
row_list = []
max_index = 11
while max_index <= len(tokens):
    row = [tokens[i] for i in np.arange(max_index - 11, max_index)]
    row_list.append(row)
    max_index += 11

# Create dataframe from array of arrays, save proper CSV, you're done.
```

## 2 - Optimize data load

### Happy path

Create a permanent or temporary Heap table to more rapidly ingest data into the SQL pool, then insert into the existing table.

Since this is a "raw import", when they are ready, they will likely use T-SQL COPY statements to import into the staging (Heap) table, then insert that into the existing table. COPY is recommended since the CSV files have nonstandard line endings, which PolyBase cannot handle.

### Coach's notes

Have attendees show you the following before you sign off on this challenge:

* The proper table structures and pipelines have been created and used

### Things to watch out for

| Name | Description |
| --- | --- |
| Right types of tables | Use heap staging heap tables to import data into the SQL pool then CTAS queries to move data into the final location.
| Gotchas for data flows, pipelines | Predictable performance of pipelines, time cap on the execution time of pipelines. |

## 3 - Optimize performance queries

### Happy path

Optimize the structure of the `wwi_poc.Sales_SUFFIX` ((where `SUFFIX` is the **student ID**)), `wwi_poc.Customer`, `wwi_poc.Date`, `wwi_poc.Product` tables.

Use performance optimization techniques to improve the performance of the queries.

### Coach's notes

Have attendees show you the following before you sign off on this challenge:

* The `wwi_poc.Sales_SUFFIX`, `wwi_poc.Customer`, `wwi_poc.Date`, `wwi_poc.Product` have the correct distribution (hash for `Sales_SUFFIX` and replicated for `Customer`, `Date`, `Product`) and the correct indexing (CCI for `wwi_poc.Sales_SUFFIX`)
* Performance optimization techniques used for the queries

### Things to watch out for

| Name | Description |
| --- | --- |
Sub-optimal table structures | The `wwi_poc.Sales_SUFFIX`, `wwi_poc.Customer`, `wwi_poc.Date`, `wwi_poc.Product` have suboptimal structures.


Possible solution for table structures:

```sql
CREATE TABLE wwi_poc.Customer_NNNNNN_Optimal
WITH
(
	DISTRIBUTION = REPLICATE
)
AS
SELECT
    *
FROM
    wwi_poc.Customer_NNNNNN


CREATE TABLE wwi_poc.Product_NNNNNN_Optimal
WITH
(
	DISTRIBUTION = REPLICATE
)
AS
SELECT
    *
FROM
    wwi_poc.Product_NNNNN


CREATE TABLE wwi_poc.Date_NNNNNN_Optimal
WITH
(
	DISTRIBUTION = REPLICATE
)
AS
SELECT
    *
FROM
    wwi_poc.Date_NNNNN


CREATE TABLE wwi_poc.Sale_NNNNNN_Optimal
WITH
(
	DISTRIBUTION = HASH (CustomerId),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT
    *
FROM
    wwi_poc.Sale_NNNNNN
```
