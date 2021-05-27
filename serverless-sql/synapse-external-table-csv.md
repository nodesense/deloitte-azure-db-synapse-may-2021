```sql
-- serverless sql pool 
-- while quering, it allocate resource, we pay for amount of data processed. 

-- meta data  - is managed by synapse

-- unmanaged table , is managed by you, or ETL, or apps

-- if you want to add records,  you dump a csv/parquet/json files into target folder
-- if you want to remove, you remove the files/folder in the target folder
-- if you want to update it, replace the file with new file..

-- done already

CREATE DATABASE moviedb; 

-- To select movie db use below statement, or use the UI, top side, use database select box
USE moviedb; 


-- 1. We have file formats..
-- 2. integrated data source - contains location details of the data/blob/storage account/containers
-- 3. The meta data table, external table
-- 4. data itself in rest in lake


-- 1. We have file formats..DEFINE FORMAT, csv format

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                )

-- to check whether external file format created or not..

SELECT * from sys.external_file_formats;


-- 2. integrated data source - contains location details of the data/blob/storage account/containers
-- DEFINE THE DATA SOURCE , in a Blob storage account

-- if we have ADLS, we have two options, go by File API/DFS, BLOB Storage API
-- file api, DFS api

-- storage account deloittesynapasestorage
-- container movieset
-- abfs - Azure Blob File System / No  TLS
-- abfss - Azure Blob File System Secured /TLS

CREATE EXTERNAL DATA SOURCE [tags_ds]
  WITH (LOCATION='abfss://movieset@deloittesynapasestorage.dfs.core.windows.net')

SELECT * from sys.external_data_sources;


-- 3. The meta data table, external table
-- create external table, schema located in sql serverless + data located in lake
-- define a structure for data lake file as table, useful for query, join, projection...
-- data always maanged externally by  you or tool not by Synapse

-- userId,movieId,tag,timestamp

CREATE EXTERNAL TABLE tags (
    userId INT,
    movieId INT,
    tag VARCHAR(256),
    timestamp BIGINT
) WITH (
    -- location/path within data source
    LOCATION='tags/tags.csv',
    -- data source has container name, storage account
    DATA_SOURCE = [tags_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from tags;


-- NOT SUPPORTED
--ALTER EXTERNAL DATA SOURCE [tags_ds] SET 
--    LOCATION='abfss://movieset@deloittesynapasestorage.dfs.core.windows.net'


-- DROP EXTERNAL TABLE table_name;
-- DROP EXTERNAL DATA SOURCE [datasource_name];
-- DROP EXTERNAL FILE  FORMAT [external_format_name];


CREATE EXTERNAL DATA SOURCE [movieset_ds]
  WITH (LOCATION='abfss://movieset@deloittesynapasestorage.dfs.core.windows.net')

SELECT * from sys.external_data_sources;

-- links movieId,imdbId,tmdbId

CREATE EXTERNAL TABLE links (
    movieId INT,
    imdbId INT,
    tmdbId INT
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='links/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * FROM links;

-- movies: movieId,title,genres

CREATE EXTERNAL TABLE movies (
    movieId INT,
    title VARCHAR(256),
    genres VARCHAR(256)
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='movies/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * FROM movies;

 -- ratings: userId,movieId,rating,timestamp


CREATE EXTERNAL TABLE ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp BIGINT 
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='ratings/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * FROM ratings;

-- DATA LAKE, SCHEMA STRUCTURE, DATA READY
-- OLAP - Online Analytics Processing using Serverless SQL Pool

SELECT TOP 10 * from movies;

SELECT TOP 10 UPPER(title) from movies;

-- select expression
SELECT movieId, rating, (rating + .02) as adj_rating from ratings; 

-- group by , ROLLUP

ROLLUP (country, state, store)

 -- combination 

-- USA,CO,WALLMART1
-- USA,CO,NULL
-- USA,NULL,NULL
-- NULL,NULL,NULL

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating FROM ratings GROUP BY ROLLUP (movieId);

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating FROM ratings GROUP BY (movieId);

-- where for normal filter without aggregation

SELECT movieId, rating from ratings where rating < 1.0;


-- HAVING applying condition on aggregated columns

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100;


-- GROUP BY, HAVING, ORDER BY 
SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;

```
