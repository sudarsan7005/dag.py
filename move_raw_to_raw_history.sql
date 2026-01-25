sql=

DECLARE sDataSourceRawValue STRING;
DECLARE sDataSourceRawHistValue STRING;
DECLARE sCountryKeyField STRING;
DECLARE sCommaSepCountries STRING;

DECLARE iRegionId INT64;          -- Region Id to process
DECLARE sCommaSepFields STRING;   -- Comma separated fields to the data source table query
DECLARE sQuery STRING;            -- String that contains the query to be executed by command.
DECLARE iIndex INT64 DEFAULT 1;   -- Array index to the loop
DECLARE iDataSource INT64;        -- Each value of DataSource array 
DECLARE aRawDataSource STRING; 
DECLARE sComponentName STRING;

DECLARE sRegionKey STRING DEFAULT '{requested_region}';
DECLARE sDataSourceKey STRING DEFAULT '{requested_datasource}';
DECLARE sDateToExec STRING DEFAULT '{requested_date}';
DECLARE sComponent STRING DEFAULT '{requested_datasource_component}';
DECLARE sRequestedCountry STRING DEFAULT '{requested_country}';

DECLARE aActiveTables DEFAULT (SELECT ARRAY(
    SELECT AS STRUCT DataSourceId, DataSourceRawValue,  DataSourceRawHistValue, CountryKeyField
    FROM `{landed.tables.data_source}` WHERE DataSourceName = sDataSourceKey AND Active = True AND LOWER(Region) = LOWER(sRegionKey) ORDER BY DataSourceId));

-- Change sDateToExec to today in non-production stages
IF ({datalake_mocked_flag}) THEN  
   SET sDateToExec = STRING(CURRENT_DATE());
END IF;

-- Gets the comma separated countries of country table depending RegionId
SET iRegionId = (SELECT RegionId FROM `{landed.tables.region}` WHERE LOWER(RegionKey) = (sRegionKey));
SET sCommaSepCountries = (SELECT STRING_AGG(CONCAT("'",CountryKey,"'")) FROM `{landed.tables.country}` WHERE RegionId = iRegionId AND Active = True);

-- If the requested_country parameter is having any country passed, either a single country or comma separated countries, it will process the data for those countries only
IF (sRequestedCountry != '') THEN
  SET sCommaSepCountries = (
    SELECT STRING_AGG(CONCAT("'", TRIM(country), "'"), ',')
    FROM UNNEST(SPLIT(UPPER(sRequestedCountry), ',')) AS country
  );
END IF;
 

WHILE iIndex <= ARRAY_LENGTH(aActiveTables) 
DO

  SET (iDataSource, sDataSourceRawValue, sDataSourceRawHistValue, sCountryKeyField) = aActiveTables[ordinal(iIndex)]; -- Sets each element in iFrequency variable

  -- If the component is informed, we only process the data_source of it
  IF LENGTH(sComponent) > 0 AND NOT `{landed.dataset}`.IsValidCompenentDataSource(iRegionId, sComponent, iDataSource) THEN
    SET iIndex = iIndex + 1;
    CONTINUE;
  END IF; 
  
  SET sCommaSepFields = (
  SELECT STRING_AGG(column_name, ', ') FROM `{raw.dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
  WHERE table_name = SPLIT(sDataSourceRawValue, '.')[OFFSET(2)]);
  
  -- SET sComponentName = (SELECT ComponentName FROM `{landed.tables.component_datasource_mapping}` WHERE DataSourceId = iDataSource);
 ------------------------------
  -- Clean data for execution --
  ------------------------------
  SET sQuery = concat('DELETE FROM `', sDataSourceRawHistValue, '` WHERE createddttm = DATE(\'' , sDateToExec , '\') AND ',
                        '`{insights.dataset}`.GetValidCountryKey(',sCountryKeyField,') IN ( ', sCommaSepCountries ,' )');
  
  EXECUTE IMMEDIATE (sQuery);

  -------------------------------
  -- Insert data for execution --
  -------------------------------  

  SET sQuery = concat('INSERT `', sDataSourceRawHistValue, '` (', sCommaSepFields  , ', createddttm) SELECT ', sCommaSepFields, 
                      ', DATE(\'' , sDateToExec , '\') AS createddttm', 
                      ' FROM `',  sDataSourceRawValue, '` AS T',
                      ' WHERE `{insights.dataset}`.GetValidCountryKey(',sCountryKeyField,') ',' IN ( ', sCommaSepCountries ,' )');                     
    
  
  EXECUTE IMMEDIATE (sQuery); 
  
  SET iIndex = iIndex + 1;

END WHILE;
