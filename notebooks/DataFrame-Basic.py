# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), # orphan record, no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]
 

# COMMAND ----------

# create dataframe with columns "product_id", "product_name", "brand_id", products list as data
productDf = spark.createDataFrame(data=products, 
                                  schema=["product_id", "product_name", "brand_id"])
productDf.printSchema() # print Dataframe schema, data frame is strucutred
productDf.show() # print 20 results

# COMMAND ----------

brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])
brandDf.printSchema()
brandDf.show()

# COMMAND ----------

# dataFrame is just an api, underneath we have RDD Row
print (productDf.rdd.getNumPartitions())
print (productDf.rdd.glom().collect())

# COMMAND ----------

# DataFrame API
# DF APIs are executed by  SQL Engine

# DataFrmae Python API
# DF are immutable, same as RDD also immutable
# return new dataframe
filterDf = productDf.filter ( productDf["brand_id"] == 100 )
filterDf.show()

# COMMAND ----------

# where and filter are same..
productDf.where ( productDf.brand_id > 100 ).show()

# COMMAND ----------

# you can also use SQL in filter/where..
# since all DF/SQL will be treated by SQL Engine only
productDf.where (" brand_id = 100 AND product_id = 1 ").show() # Spark SQL Dialect

# COMMAND ----------

import pyspark.sql.functions as F

productDf.sort("product_name").show()


# COMMAND ----------

productDf.sort(F.desc("product_name")).show() # sort by decending order

# COMMAND ----------

productDf.select("product_id", "product_name").show()

# COMMAND ----------

# add new/derive a column
productDf.withColumn("name", F.upper( F.col("product_name") )).show()

# COMMAND ----------

productDf.withColumnRenamed("product_name", "name").show()

# COMMAND ----------

# \ line continuation, no space after \
productDf.withColumnRenamed("product_name", "name")\
         .filter( F.col ("name").contains("me") ).show()

# COMMAND ----------

# drop the columns

productDf.show()

newDf = productDf.drop("brand_id")

newDf.show()

# COMMAND ----------


