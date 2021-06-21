Input file format

```json
{"id":1,"email":"isidro_von@hotmail.com","first":"Torrey","last":"Veum","company":"Hilll, Mayert and Wolf","created_at":"2014-12-25T04:06:27.981Z","country":"Switzerland"}
{"id":2,"email":"frederique19@gmail.com","first":"Micah","last":"Sanford","company":"Stokes-Reichel","created_at":"2014-07-03T16:08:17.044Z","country":"Democratic People's Republic of Korea"}
```



```sql
-- Json is not supported by synapse as external table, instead use OpenRowSet

select top 10 *
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/advworks/json-customer-line-delimited.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
 
      

 select
    JSON_VALUE(doc, '$.email') AS email,
    CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    doc
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/advworks/json-customer-line-delimited.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
order by JSON_VALUE(doc, '$.id') desc
```
