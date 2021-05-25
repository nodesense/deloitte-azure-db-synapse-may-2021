# Databricks notebook source
from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType
# create instance for StructType and define schema for movie
# this way, no need to inferSchema from content, flexible, configurable
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\

# schema for rating
ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", LongType(), True)

# COMMAND ----------

# attach custom schema
movieDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(movieSchema)\
          .load("/mnt/movielens/movies.csv")

movieDf.show(2)

ratingDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(ratingSchema)\
          .load("/mnt/movielens/ratings.csv")

ratingDf.show(2)

# COMMAND ----------

# Spark SQL , DF 
# it creates a view in SQL, which can be queried usign sql statement
# temp view means, it resides only within this spark driver/app, context/session
movieDf.createOrReplaceTempView("movies")
ratingDf.createOrReplaceTempView("ratings")

# COMMAND ----------

# query using sql
"""
spark- Spark Session, entry point for DF, SQL engines
"""

df = spark
.sql("select * from movies")
df.printSchema()
df.show(2)

# COMMAND ----------

spark.sql("select * from ratings").show(2)

# COMMAND ----------

spark.sql("select movieId, rating, rating * 2 as rating10 from ratings").show(2)

# COMMAND ----------

spark.sql("select movieId, upper(title) from movies").show(3)

# COMMAND ----------

df = spark.sql("""
          SELECT movieId, 
                 count(userId) as total_ratings, 
                 avg(rating) as avg_ratings
          FROM 
                 ratings
          GROUP BY 
                 movieId
                 
          HAVING total_ratings >= 100
          
          ORDER BY total_ratings DESC
""")

df.show()
# if we again need agg df to be sql, we need to again create a temp view
df.createOrReplaceTempView("rating_agg")
# select * from rating_agg

# COMMAND ----------

#  CREATE OR REPLACE TEMP VIEW agg_ratings, will create a sql temp view
spark.sql("""
          CREATE OR REPLACE TEMP VIEW agg_ratings AS
          SELECT movieId, 
                 count(userId) as total_ratings, 
                 avg(rating) as avg_ratings
          FROM 
                 ratings
          GROUP BY 
                 movieId
                 
          HAVING total_ratings >= 100
          
          ORDER BY total_ratings DESC
""")

# COMMAND ----------

spark.sql('select * from agg_ratings').show(5)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- magic function, the code here executed by spark.sql only.. and used with display
# MAGIC select * from agg_ratings

# COMMAND ----------



# COMMAND ----------

spark.sql("""
   CREATE OR REPLACE TEMP VIEW most_popular_movies AS 
   SELECT r.movieId, title, total_ratings, avg_ratings from agg_ratings r
   JOIN movies m on r.movieId = m.movieId
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM most_popular_movies LIMIT 10

# COMMAND ----------

# we get dataframe from table
# temp table or temp view in spark are same.
resultDf = spark.table("most_popular_movies")
resultDf.printSchema()
resultDf.show(2)

# COMMAND ----------

resultDf.coalesce(1).write.mode("overwrite").option("header", True)\
                              .csv("/mnt/movielens/results-sql-to-csv")

# COMMAND ----------


