```sql


-- dedicated pool cannot access formats, tables, from sql serverless
-- dedicated pool can have external tables of itself, can be linked to data lake, used for joins, aggregation..

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                )
 
 
CREATE EXTERNAL DATA SOURCE [movieset_ds]
  WITH (LOCATION='abfss://movieset@deloittesynapasestorage.dfs.core.windows.net', 
        TYPE=HADOOP)


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

SELECT * from links;


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
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from tags;


```
