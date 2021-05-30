
In synapse dedi pool,

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'SomeComlextKey###$';

In databricks,

```python
storageKey = "storage-key-1"
# Otherwise, set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.deloittesynapasestorage.blob.core.windows.net",
  storageKey)
  
jdbcUrl = "jdbc:sqlserver://<<server>>.sql.azuresynapse.net;database=<<database>>;user=sqladminuser;password=<<password>>;"
  
tempDir = "wasbs://temp@<<storage>>.blob.core.windows.net/"
```

```

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcUrl) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "category") \
  .load()


df.show()
  ```
  
# Load data from an Azure Synapse query.

```  
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcUrl) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", "select * from category") \
  .load()
  
df.show()
```

## now write back.

```
category = [
    #(id, category_name)
    (100, "Books"),
    (200, "Kitchen"),
    (400, "Home"),
    (500, "Utils"),
]
 
categoryDf = spark.createDataFrame(data=category, schema=["id", "category_name"])

categoryDf.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcUrl) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "category") \
  .mode("append") \
  .save()
  
```
