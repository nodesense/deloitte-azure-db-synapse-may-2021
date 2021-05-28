```sql
-- PArquest table 

 
create external data source ecommerce_agg_ds
with ( location = 'abfss://ecommerce@deloittesynapasestorage.dfs.core.windows.net' );

CREATE EXTERNAL FILE FORMAT PARQUET_FORMAT
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
 

-- ROP EXTERNAL TABLE agg_amount;

CREATE EXTERNAL TABLE agg_amount (
    InvoiceNo VARCHAR(256),
    Amount float
) WITH (
    LOCATION = 'agg-results',
    DATA_SOURCE = ecommerce_agg_ds,  
    FILE_FORMAT = PARQUET_FORMAT
);

select * from agg_amount;
```
