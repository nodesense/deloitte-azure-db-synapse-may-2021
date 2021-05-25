# Databricks notebook source
# RDD, Resilient Distributed Data Set

data = [1,2,3,4,5,6,7,8,9]
# Spark Context
rdd1 = sc.parallelize(data) # load the input into cluster memory

print("max ", rdd1.max())

# to run it, Shift + Enter key

# COMMAND ----------

print("Min", rdd1.min())
print("mean", rdd1.mean())
print("sum", rdd1.sum())

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://movielens@gksynapse2storage.blob.core.windows.net",
  mount_point = "/mnt/movielens")


# COMMAND ----------


