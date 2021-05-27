```sql

-- partitions, how the data is stored further in the distribution


-- DISTRUBED ROUND_ROBIN, REPLICATE, HASH

-- How the data stored with in distribution
-- ROWSTORE, COLUMNSTORE

-- Partition - within the ROWSTORE, COLUMNSTORE, how the data is split again into chunk
    -- LEFT RANGE
    -- RIGHT  RANGE
  -- by default it will assum round round robin for distribution

CREATE TABLE users(
    id INT NOT NULL,
    firstname varchar(20)
) WITH (
    PARTITION (id RANGE LEFT FOR VALUES (1000, 10000, 20000, 30000))
)

-- partition 1
INSERT INTO users VALUES (1, 'Ramesh');
INSERT INTO users VALUES (2, 'Amala');
INSERT INTO users VALUES (3, 'Paul');
INSERT INTO users VALUES (4, 'Ravi');
INSERT INTO users VALUES (5, 'Karthik');
INSERT INTO users VALUES (6, 'Ali');
-- parition 2
INSERT INTO users VALUES (1500, 'Iniya');
-- partition 5
INSERT INTO users VALUES (30010, 'Venkat');



```
