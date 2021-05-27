```sql

-- GROUP BY, HAVING, ORDER BY 
-- external table with in dedicated sql

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;

-- query from dedi table/Internal

SELECT TOP 10 id, title  from movies 

-- POLY BASE Query, multiple flavors, csv/parquet, orc
-- join internal and external tables..


SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
JOIN movies 
ON movies.id = ratings.movieId
GROUP BY movieId, title
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;

-- CTAS - Create Table As Select

-- we create a dedi pool table called popular_movies
-- from the query 

CREATE TABLE popular_movies 
WITH(
    DISTRIBUTION=REPLICATE, 
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
    FROM ratings 
    JOIN movies 
    ON movies.id = ratings.movieId
    GROUP BY movieId, title
    HAVING COUNT(userId) >= 100;

SELECT * from popular_movies;

```
