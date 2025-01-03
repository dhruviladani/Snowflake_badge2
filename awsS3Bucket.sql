create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';

list @util_db.public.aws_s3_bucket;

copy into INTL_DB.PUBLIC.INT_STDS_ORG_3166
from @util_db.public.aws_s3_bucket
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name='util_db.public.PIPE_DBLQUOTE_HEADER_CR' );

select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
 SELECT 'CMCW01' as step
 ,( select count(*) 
   from snowflake.account_usage.databases
   where database_name = 'INTL_DB' 
   and deleted is null) as actual
 , 1 as expected
 ,'Created INTL_DB' as description
 );

select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW02' as step
 ,( select count(*) 
   from INTL_DB.INFORMATION_SCHEMA.TABLES 
   where table_schema = 'PUBLIC' 
   and table_name = 'INT_STDS_ORG_3166') as actual
 , 1 as expected
 ,'ISO table created' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
SELECT 'CMCW03' as step 
 ,(select row_count 
   from INTL_DB.INFORMATION_SCHEMA.TABLES  
   where table_name = 'INT_STDS_ORG_3166') as actual 
 , 249 as expected 
 ,'ISO Table Loaded' as description 
); 

select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;

create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
( iso_country_name
  ,country_name_official
  ,alpha_code_2digit
  ,region) AS
  select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey

;

select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW04' as step
 ,( select count(*) 
   from INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO) as actual
 , 249 as expected
 ,'Nations Sample Plus Iso' as description
);

create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

  create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';

create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 
;

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW05' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'COUNTRY_CODE_TO_CURRENCY_CODE') as actual
 , 265 as expected
 ,'CCTCC Table Loaded' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW06' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'CURRENCIES') as actual
 , 151 as expected
 ,'Currencies table loaded' as description
);

create view intl_db.public.simple_currency
( cty_code, cur_code) AS select COUNTRY_CHAR_CODE, CURRENCY_CHAR_CODE from INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE;

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
 SELECT 'CMCW07' as step 
,( select count(*) 
  from INTL_DB.PUBLIC.SIMPLE_CURRENCY ) as actual
, 265 as expected
,'Simple Currency Looks Good' as description
);

alter view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
set secure;

alter view intl_db.public.SIMPLE_CURRENCY
set secure; 
