# Databricks notebook source
data = [100,200, 300, 400, 500, 600, 700]
rdd = sc.parallelize(data)

# COMMAND ----------

# may vary based on runtime configuration
print(rdd.getNumPartitions()) 

# COMMAND ----------

#  glom collect the data from paritions
partitionData = rdd.glom().collect() # returns list of list of parition data
print(partitionData)

# COMMAND ----------

data = [100,200, 300, 400, 500, 600, 700]
# pre-configure partitions
rdd = sc.parallelize(data, 1)
print (rdd.getNumPartitions())
print (rdd.glom().collect())

# COMMAND ----------

data = [100,200, 300, 400, 500, 600, 700]
# pre-configure partitions
rdd = sc.parallelize(data, 2)
print (rdd.getNumPartitions())
print (rdd.glom().collect())

# number of partitions = number of cores * 2 or 3 

# COMMAND ----------


