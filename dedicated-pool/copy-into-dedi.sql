```

-- create internal/managed table in dedicated pool

CREATE TABLE movies (
    id INT NOT NULL,
    title VARCHAR(500),
    genres VARCHAR(250)
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- https://storageacc.blob.core.windows.net/movieset/movies/movies.csv
-- copy all columns

COPY INTO movies 
    FROM 'https://storageacc.blob.core.windows.net/movieset/movies/movies.csv'
WITH (
FILE_TYPE = 'CSV',
FIELDTERMINATOR = ',',
FIRSTROW=2
)

SELECT * from movies;

```
