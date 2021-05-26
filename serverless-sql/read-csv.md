```sql

-- This is auto-generated code, picks all data from file
-- headers are included as row
-- select a query and run it.

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://yourstorage.dfs.core.windows.net/movieset/links/links.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0'
    ) AS [result]

-- SKIP the header, set a offset for the first rwo
-- column names appear as C1, C2..

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://yourstorage.dfs.core.windows.net/movieset/links/links.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        FIRSTROW = 2
    ) AS [result]


-- use the column name from CSV


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://yourstorage.dfs.core.windows.net/movieset/links/links.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        HEADER_ROW = TRUE
    ) AS [result]


```
