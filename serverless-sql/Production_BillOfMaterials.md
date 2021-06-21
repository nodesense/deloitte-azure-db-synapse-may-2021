File Source: Production_BillOfMaterials.CSV


```csv
"BillOfMaterialsID","ProductAssemblyID","ComponentID","StartDate","EndDate","UnitMeasureCode","BOMLevel","PerAssemblyQty","ModifiedDate"
"893","","749","5/26/2010 12:00:00 AM","","EA ","0","1.00","5/12/2010 12:00:00 AM"
"271","","750","3/4/2010 12:00:00 AM","5/3/2010 12:00:00 AM","EA ","0","1.00","5/3/2010 12:00:00 AM"
"34","","750","5/4/2010 12:00:00 AM","","EA ","0","1.00","4/20/2010 12:00:00 AM"
"830","","751","5/26/2010 12:00:00 AM","","EA ","0","1.00","5/12/2010 12:00:00 AM"
"2074","","752","7/8/2010 12:00:00 AM","","EA ","0","1.00","6/24/2010 12:00:00 AM"
"1950","","753","6/19/2010 12:00:00 AM","8/18/2010 12:00:00 AM","EA ","0","1.00","8/18/2010 12:00:00 AM"
"1761","","753","8/19/2010 12:00:00 AM","","EA ","0","1.00","8/5/2010 12:00:00 AM"
"3088","","754","12/15/2010 12:00:00 AM","","EA ","0","1.00","12/1/2010 12:00:00 AM"
"3351","","755","12/23/2010 12:00:00 AM","","EA ","0","1.00","12/9/2010 12:00:00 AM"
"3246","","756","12/23/2010 12:00:00 AM","","EA ","0","1.00","12/9/2010 12:00:00 AM"
"2760","","757","9/15/2010 12:00:00 AM","","EA ","0","1.00","9/1/2010 12:00:00 AM"
"2395","","758","8/5/2010 12:00:00 AM","","EA ","0","1.00","7/22/2010 12:00:00 AM"
"3087","","759","12/15/2010 12:00:00 AM","","EA ","0","1.00","12/1/2010 12:00:00 AM"
"3350","","760","12/23/2010 12:00:00 AM","","EA ","0","1.00","12/9/2010 12:00:00 AM"
"2822","","761","9/15/2010 12:00:00 AM","","EA ","0","1.00","9/1/2010 12:00:00 AM"
"3245","","762","12/23/2010 12:00:00 AM","","EA ","0","1.00","12/9/2010 12:00:00 AM"
"2759","","763","9/15/2010 12:00:00 AM","","EA ","0","1.00","9/1/2010 12:00:00 AM"
"2394","","764","8/5/2010 12:00:00 AM","","EA ","0","1.00","7/22/2010 12:00:00 AM"
"3341","","765","12/23/2010 12:00:00 AM","","EA ","0","1.00","12/9/2010 12:00:00 AM"
"2815","","766","9/15/2010 12:00:00 AM","","EA ","0","1.00","9/1/2010 12:00:00 AM"
"2449","","767","8/5/2010 12:00:00 AM","","EA ","0","1.00","7/22/2010 12:00:00 AM"

```





-- 5/26/2010 12:00:00 AM
-- dd/MM/yyyy HH:mm:ss tt

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeaderWithDblQuote] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',' , 
                                    FIRST_ROW=2, 
                                    STRING_DELIMITER = '0x22',
                                    DATE_FORMAT = 'dd/MM/yyyy HH:mm:ss tt'
                                    )
                );

SELECT * from sys.external_data_sources;

  
CREATE EXTERNAL DATA SOURCE [advworks_ds]
  WITH (LOCATION='abfss://advworks@ibmbatch02synapse.dfs.core.windows.net')
 

SELECT * from sys.external_data_sources;
 
DROP EXTERNAL TABLE Production_BillOfMaterials;

CREATE EXTERNAL TABLE Production_BillOfMaterials (
    BillOfMaterialsID INT,
    ProductAssemblyID INT,
    ComponentID INT,
    StartDate DATETIME,
    EndDate DATETIME,
    UnitMeasureCode VARCHAR(100),
    BOMLevel INT,
    PerAssemblyQty FLOAT,
    ModifiedDate DATETIME
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='/Production_BillOfMaterials.csv',
    -- data source has container name, storage account
    DATA_SOURCE = [advworks_ds],
    FILE_FORMAT = [ExternalCSVWithHeaderWithDblQuote]
);

SELECT * from Production_BillOfMaterials;
