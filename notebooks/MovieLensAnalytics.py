# Databricks notebook source
# we already mounted /mnt/movielens
# reading from blob storage
# inferSchema is useful to extract the schema by looking into the content
# expensive, it scan the content, very limited.. 
# dont use inferSchema, instead define the schema per data model
movieDf = spark.read.option("header","true")\
                    .option("inferSchema", "true")\
                    .csv("/mnt/movielens/movies.csv")

movieDf.printSchema()
movieDf.show(2)

# COMMAND ----------

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

# count 
print (ratingDf.count())

# COMMAND ----------

# to get schema
print (ratingDf.schema)

# COMMAND ----------

# to get all columns
print (ratingDf.columns)

# COMMAND ----------

import  pyspark.sql.functions as F

ratingDf.filter( (F.col("rating") >=3) & (F.col("rating") <=4)).show(20)

# COMMAND ----------

# aggregation with groupBy
# get list of movies, voted by more users, not based ratings..
mostPopularMoviesDf = ratingDf\
                      .groupBy("movieId")\
                      .agg(F.count("userId").alias("total_ratings"), 
                           F.avg("rating").alias("avg_ratings"))\
                      .filter (F.col("total_ratings") >= 100)\
                      .sort(F.desc("total_ratings"))

mostPopularMoviesDf.printSchema()
mostPopularMoviesDf.show(200)

# COMMAND ----------

movieDf.show(1)

# join two DF, inner join
mostPopularMoviesTitleDf = mostPopularMoviesDf\
                           .join(movieDf, "movieId")
mostPopularMoviesTitleDf.show()

# COMMAND ----------

# join two DF, inner join
mostPopularMoviesTitleDf = mostPopularMoviesDf\
                           .join(movieDf, mostPopularMoviesDf.movieId == movieDf.movieId)\
                           .select(mostPopularMoviesDf.movieId, "title", 
                                   "total_ratings", "avg_ratings")

mostPopularMoviesTitleDf.show()

# COMMAND ----------

# need write permission to container
# you may find many files in folder due to paritions created by groupBy, typically 200
mostPopularMoviesTitleDf.write.mode("overwrite")\
                              .csv("/mnt/movielens/results-csv")
# now check in azure storage blobs container

# COMMAND ----------

# move results into single csv file..
#coalesce, useful to the reduce the paritions with possibly less suffling
# data from multiple paritions shall be moved into single partition
# then we write teh single parition data into azure blob storage..
mostPopularMoviesTitleDf.coalesce(1).write.mode("overwrite").option("header", True)\
                              .csv("/mnt/movielens/results-single-csv")
# now check in azure storage blobs container

# COMMAND ----------


