```sql

-- Dedicated Pool
-- Column Store, more efficient, scan only specific columns instead whole rows
-- queries can run faster, interactive, good for dashboard, notebooks
-- Managed table, DW Table
-- the table DDL, table data is managed by Dedicated Pool
-- insert, delete, update, create, drop all these to be done on Dedicated Pool as SQL

-- Distribution : how the table data to be stored in compute nodes 
-- REPLICATE, ROUND_ROBIN, HASH

-- with column store

CREATE TABLE category (
    id INT NOT NULL,
    category_name VARCHAR(100)
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);


INSERT INTO category VALUES (1, 'mobile');
INSERT INTO category VALUES (2, 'tablet');

SELECT * From category;

CREATE TABLE sales (
    id INT NOT NULL,
    amount FLOAT, 
    country VARCHAR(100),
    category INT
) WITH (
     DISTRIBUTION = HASH(country),
     CLUSTERED COLUMNSTORE INDEX
)

INSERT INTO sales VALUES (1, 100, 'IN', 1);
INSERT INTO sales VALUES (2, 200, 'USA', 2);
INSERT INTO sales VALUES (3, 300, 'CA', 1);
INSERT INTO sales VALUES (4, 600, 'IN', 2);
INSERT INTO sales VALUES (5, 700, 'CA', 2);
INSERT INTO sales VALUES (6, 800, 'CA', 1);

select * from sales;

-- DROP TABLE category;

-- doesn't work
-- DESCRIBE TABLE  category;



```
