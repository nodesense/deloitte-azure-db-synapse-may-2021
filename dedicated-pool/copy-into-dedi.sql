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

-- create internal/managed table in dedicated pool

CREATE TABLE movies (
    id INT NOT NULL,
    title VARCHAR(500),
    genres VARCHAR(250)
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- https://deloittesynapasestorage.blob.core.windows.net/movieset/movies/movies.csv
-- copy all columns

COPY INTO movies 
    FROM 'https://deloittesynapasestorage.blob.core.windows.net/movieset/movies/movies.csv'
WITH (
FILE_TYPE = 'CSV',
FIELDTERMINATOR = ',',
FIRSTROW=2
)

SELECT * from movies;

-- copy into specific columns

CREATE TABLE movies2 (
    id INT NOT NULL,
    title VARCHAR(500)
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);


COPY INTO movies2 (id, title)
    FROM 'https://sto.blob.core.windows.net/movieset/movies/movies.csv'
WITH (
FILE_TYPE = 'CSV',
FIELDTERMINATOR = ',',
FIRSTROW=2
)

SELECT * from movies2;
