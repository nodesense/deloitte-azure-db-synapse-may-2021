-- Create Master Key 

create master key encryption by password = 'MyTest!Mast3rP4ss';


CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                )

-- CREATE SAS key with READ and LIST permission               

CREATE DATABASE SCOPED CREDENTIAL [MovieLensCredentials]
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = 'sp=rl&st=2021-06-21T16:14:40Z&se=2021-06-27T00:14:40Z&spr=https&sv=2020-02-10&sr=c&sig=0T0hIvk%2B5LgkU5rAohEfhxVHndZIH1cUfLDr3x9mMyk%3D';

 

CREATE EXTERNAL DATA SOURCE [movielens_ds]
  WITH (LOCATION='abfss://movielens@ibmbatch02storage.dfs.core.windows.net', 
        CREDENTIAL = MovieLensCredentials
        )




CREATE EXTERNAL TABLE movielens_links (
    movieId INT,
    imdbId INT,
    tmdbId INT
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='links/',
    -- data source has container name, storage account
    DATA_SOURCE = [movielens_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);


SELECT * from movielens_links;


DROP EXTERNAL TABLE movielens_links;

DROP EXTERNAL DATA SOURCE [movielens_ds];

DROP DATABASE SCOPED CREDENTIAL [MovieLensCredentials];

 
