# Databricks notebook source
from pyspark.sql.types import DoubleType,StructType, StringType, IntegerType, DateType
import pyspark.sql.functions as F

schema = StructType() \
         .add("InvoiceNo", StringType(), True) \
         .add("StockCode", StringType(), True) \
         .add("Description", StringType(), True) \
         .add("Quantity", IntegerType(), True) \
         .add("InvoiceDate", DateType(), True) \
         .add("UnitPrice", DoubleType(), True) \
         .add("CustomerID", IntegerType(), True) \
         .add("Country", StringType(), True)

# COMMAND ----------

# No Job Created
dataSet = spark.read.format("csv") \
                .option("header", True) \
                .schema(schema) \
                .option("dateFormat", "MM/dd/yyyy HH:mm")\
                .load("/mnt/ecommerce/raw")



# COMMAND ----------

# no job created..lazy
sorteDf = dataSet.sort(F.desc("Quantity"))
filteredDf = sorteDf.filter ("CustomerID IS NOT NULL")\
                    .filter ("InvoiceNo IS NOT NULL")\
                    .filter("Quantity IS NOT NULL")\
                    .filter("UnitPrice IS NOT NULL")\

# COMMAND ----------

# no job created, lazy
withAmountDf = filteredDf.withColumn("Amount", F.col("Quantity") * F.col("UnitPrice"))
withAmountDf.printSchema()

# COMMAND ----------

# physical plan, which will be executed by spark
withAmountDf.explain()

# COMMAND ----------

# extended plan, which will be executed by spark
# Parsed Logical Plan - they way you wrote the code, and parsed by spark, it will not resolve datatypes, where column exist or not..
# Analyzed Logical Plan  - check if columns exist, detect column types as per schema
#  Optimized Logical Plan - optimzation, like sorting, filtering which shuld be first
# Physical Plan : The final plan taken by spark
withAmountDf.explain(True)

# COMMAND ----------

# lazy
dataSet.createOrReplaceTempView("invoices")

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN SELECT *, Quantity * UnitPrice AS AMOUNT from invoices where Quantity IS NOT NULL AND UnitPrice IS NOT NULL ORDER BY QUANTITY

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED SELECT *, Quantity * UnitPrice AS AMOUNT from invoices where Quantity IS NOT NULL AND UnitPrice IS NOT NULL ORDER BY QUANTITY

# COMMAND ----------

spark.sql("EXPLAIN EXTENDED SELECT *, Quantity * UnitPrice AS AMOUNT from invoices where Quantity IS NOT NULL AND UnitPrice IS NOT NULL ORDER BY QUANTITY").show()

# COMMAND ----------

dataSet.rdd.getNumPartitions()

# COMMAND ----------

# action method, this will create independent DAG, Stages, uses executors, load file from blob storage, partition data, get the result
dataSet.count() # every action is a job
# here, at this moment, all your partitions data shall be released

# COMMAND ----------

# every action method is a job
# each job shall have stages
# each stage shall have tasks, paritions as per rdd
dataSet.count() # result is not cached, it will again read the data from azure, load and count..


# COMMAND ----------

aggDf = withAmountDf.groupBy("InvoiceNo")\
            .agg(F.sum("Amount").alias("TotalAmount"))

aggDf.show(100) # action method

# COMMAND ----------

aggDf.cache() # lazy

# COMMAND ----------

# now first run, it will cache, read from disk, process, aggregate, sum.., result will be cache
aggDf.write.mode('overwrite').parquet("/mnt/ecommerce/agg-results")

# COMMAND ----------

aggDf.count() # aggDf already cached, aggregated, data in 200 partitions are cached already
# this time, it should not go azure, no filter, no more aggregation.. all done, cached

# COMMAND ----------

aggDf.rdd.getNumPartitions()


# COMMAND ----------

# move 200 partitions data into 4
aggDf.coalesce(4).write.mode('overwrite').parquet("/mnt/ecommerce/agg-results")

# COMMAND ----------


