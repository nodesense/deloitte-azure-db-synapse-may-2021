# Json Views

file  format

```
{"id":1,"email":"isidro_von@hotmail.com","first":"Torrey","last":"Veum","company":"Hilll, Mayert and Wolf","created_at":"2014-12-25T04:06:27.981Z","country":"Switzerland"}
{"id":2,"email":"frederique19@gmail.com","first":"Micah","last":"Sanford","company":"Stokes-Reichel","created_at":"2014-07-03T16:08:17.044Z","country":"Democratic People's Republic of Korea"}
```


```

-- Working with Views

DROP VIEW IF EXISTS peopleview;

CREATE VIEW peopleview AS
 select
    CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    JSON_VALUE(doc, '$.email') AS email,
    JSON_VALUE(doc, '$.first') AS first,
    JSON_VALUE(doc, '$.last') AS last,
    JSON_VALUE(doc, '$.company') AS company, 
    JSON_VALUE(doc, '$.created_at') AS created_at, 
    JSON_VALUE(doc, '$.country') AS country
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/advworks/json-customer-line-delimited.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows;


select * from peopleview;


select * from peopleview order by id desc;
```
