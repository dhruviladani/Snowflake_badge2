// How many of my deliveries will be delayed due to snowfall?
/*
When it snows in excess of six inches per day, my company experiences delivery delays. How many of my deliveries were impacted during the third week of January for the previous year?
*/
WITH timestamps AS
(   
    SELECT
        DATE_TRUNC(year,DATEADD(year,-1,CURRENT_DATE())) AS ref_timestamp,
        LAST_DAY(DATEADD(week,2 + CAST(WEEKISO(ref_timestamp) != 1 AS INTEGER),ref_timestamp),week) AS end_week,
        DATEADD(day, day_num - 7, end_week) AS date_valid_std
    FROM
    (   
        SELECT
            ROW_NUMBER() OVER (ORDER BY SEQ1()) AS day_num
        FROM
            TABLE(GENERATOR(rowcount => 7))
    ) 
)
SELECT
    country,
    postal_code,
    date_valid_std,
    tot_snowfall_in 
FROM 
    standard_tile.history_day
NATURAL INNER JOIN
    timestamps
WHERE
    country='US' AND
    tot_snowfall_in > 6.0 
ORDER BY 
    postal_code,date_valid_std
;

// Determine if an event will be impacted by rain.
/*
I am hosting an outdoor event in seven days. How can I utilize your forecast data to determine if my event will be impacted by rain?
*/
SELECT COUNTRY,DATE_VALID_STD, POSTAL_CODE, DATEDIFF(day,current_date(),DATE_VALID_STD) AS DAY, HOUR(TIME_INIT_UTC) AS HOUR, TOT_PRECIPITATION_IN FROM STANDARD_TILE.FORECAST_DAY WHERE POSTAL_CODE='32333' AND DAY=7;

// Use temperature data to create sales forecast.
/*
Our company sells 70% more product when the temperature is in excess of 80 degrees and I am trying to create a product sales forecast for this upcoming July. How can we use your climatology data to quickly ascertain how many days “normally” exceed 80 degrees during the month of July?
*/
SELECT COUNTRY, POSTAL_CODE, SUM(IFF(AVG_OF__DAILY_MAX_TEMPERATURE_AIR_F>80, 1, 0)) DaysAbove80 FROM STANDARD_TILE.CLIMATOLOGY_DAY WHERE DOY_STD>=182 AND DOY_STD<=212 AND COUNTRY='US' GROUP BY COUNTRY,POSTAL_CODE ORDER BY DaysAbove80 DESC, COUNTRY, POSTAL_CODE;

//  Can my restaurant use weather to determine the amount of footfall traffic that we will have in the next week?
/*
Our restaurant has a significant amount of outdoor dining space. We need to determine staffing and demand based on the forecasted weather for next week.
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    avg_temperature_air_2m_f,
    avg_humidity_relative_2m_pct,
    avg_wind_speed_10m_mph,
    tot_precipitation_in,
    tot_snowfall_in,
    avg_cloud_cover_tot_pct,
    probability_of_precipitation_pct,
    probability_of_snow_pct
FROM
(
    SELECT
        postal_code,
        country,
        date_valid_std,
        avg_temperature_air_2m_f,
        avg_humidity_relative_2m_pct,
        avg_wind_speed_10m_mph,
        tot_precipitation_in,
        tot_snowfall_in,
        avg_cloud_cover_tot_pct,
        probability_of_precipitation_pct,
        probability_of_snow_pct,
        DATEADD(DAY,1,CURRENT_DATE()) AS skip_date,
        DATEADD(DAY,7 - DAYOFWEEKISO(skip_date),skip_date) AS next_sunday
    FROM
        standard_tile.forecast_day
)
WHERE
    date_valid_std BETWEEN next_sunday AND DATEADD(DAY,6,next_sunday)
ORDER BY
    date_valid_std
;



------------

alter database GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI rename to WEATHERSOURCE;

SELECT * FROM WEATHERSOURCE;

select distinct country from history_day;

select count(distinct postal_code) from history_day where country = 'US' and (postal_code like '481%' or postal_code like '482%');

create database MARKETING;

create schema MAILERS;

CREATE OR REPLACE VIEW MAILERS.DETROIT_ZIPS AS
SELECT *
FROM WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY limit 9;

select count(*) from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY;

select count(*) from DETROIT_ZIPS;

CREATE OR REPLACE VIEW MAILERS.DETROIT_ZIPS AS
SELECT *
FROM WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY 
WHERE country = 'US' and (postal_code LIKE '481%' OR postal_code LIKE '482%')
LIMIT 9;

select * from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY hd join demo_db.public.detroit_zips dz on hd.postal_code=dz.postal_code;

create database demo_db;

select DATE_VALID_STD from FORECAST_DAY where DATE_VALID_STD between (SELECT MIN(DATE_VALID_STD) FROM FORECAST_DAY)
AND (SELECT MAX(DATE_VALID_STD) FROM FORECAST_DAY);

select min(DATE_VALID_STD), max(DATE_VALID_STD) from FORECAST_DAY;

select max(history_day.date_valid_std), min(history_day.date_valid_std) from history_day join marketing.mailers.detroit_zips dz on history_day.postal_code = dz.postal_code;

select max(FORECAST_DAY.date_valid_std), min(FORECAST_DAY.date_valid_std) from FORECAST_DAY join marketing.mailers.detroit_zips dz on FORECAST_DAY.postal_code = dz.postal_code;


select fd.DATE_VALID_STD, avg(fd.AVG_CLOUD_COVER_TOT_PCT) from forecast_day fd join marketing.mailers.detroit_zips dz on fd.postal_code = dz.postal_code group by fd.DATE_VALID_STD order by avg(fd.AVG_CLOUD_COVER_TOT_PCT) asc;


create database util_db;

use role accountadmin;
create or replace api integration dora_api_integration api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole' enabled = true api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');




use role accountadmin;

create or replace external function util_db.public.grader(        
 step varchar     
 , passed boolean     
 , actual integer     
 , expected integer    
 , description varchar) 
 returns variant 
 api_integration = dora_api_integration 
 context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
 as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'  
;  

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW10' as step
 ,( select count(*)
    from snowflake.account_usage.databases
    where (database_name in ('WEATHERSOURCE','INTERNATIONAL_CURRENCIES')
           and type = 'IMPORTED DATABASE'
           and deleted is null)
    or (database_name = 'MARKETING'
          and type = 'STANDARD'
          and deleted is null)
   ) as actual
 , 3 as expected
 ,'ACME Account Set up nicely' as description
); 

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW11' as step
 ,( select count(*) 
   from MARKETING.MAILERS.DETROIT_ZIPS) as actual
 , 9 as expected
 ,'Detroit Zips' as description
); 
