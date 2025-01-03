CREATE TABLE vin.decode.wmi_to_manuf 
(
     wmi	    varchar(6)
    ,manuf_id	    number(6)
    ,manuf_name	    varchar(50)
    ,country	    varchar(50)
    ,vehicle_type    varchar(50)
 );

 CREATE TABLE vin.decode.manuf_to_make
(
     manuf_id	number(6)
    ,make_name	varchar(50)
    ,make_id	number(5)
);

CREATE TABLE vin.decode.model_year
(
     model_year_code	varchar(1)
    ,model_year_name	varchar(4)
);

CREATE TABLE vin.decode.manuf_plants
(
     make_id	number(5)
    ,plant_code	varchar(1)
    ,plant_name	varchar(75)
 );

 CREATE TABLE vin.decode.make_model_vds
(
     make_id	  number(3)
    ,model_id	  number(6)
    ,model_name	  varchar(50)
    ,vds	  varchar(5)
    ,desc1	  varchar(25)
    ,desc2	  varchar(25)
    ,desc3	  varchar(50)
    ,desc4	  varchar(25)
    ,desc5	  varchar(25)
    ,body_style	  varchar(25)
    ,engine	  varchar(100)
    ,drive_type	  varchar(50)
    ,transmission varchar(50)
    ,mpg  	varchar(25)
);

CREATE FILE FORMAT vin.decode.comma_sep_oneheadrow 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
field_optionally_enclosed_by = '"'  
trim_space = TRUE;

COPY INTO vin.decode.wmi_to_manuf
from @vin.decode.aws_s3_bucket
files = ('Maxs_WMIToManuf_data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

COPY INTO vin.decode.manuf_to_make
from @vin.decode.aws_s3_bucket
files = ('Maxs_ManufToMake_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

COPY INTO vin.decode.model_year
from @vin.decode.aws_s3_bucket
files = ('Maxs_ModelYear_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

COPY INTO vin.decode.manuf_plants
from @vin.decode.aws_s3_bucket
files = ('Maxs_ManufPlants_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

COPY INTO vin.decode.MAKE_MODEL_VDS
from @vin.decode.aws_s3_bucket
files = ('Maxs_MMVDS_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

set sample_vin = 'SAJAJ4FX8LCP55916';

select $sample_vin;

SELECT $sample_vin as VIN
  , LEFT($sample_vin,3) as WMI
  , SUBSTR($sample_vin,4,5) as VDS
  , SUBSTR($sample_vin,10,1) as model_year_code
  , SUBSTR($sample_vin,11,1) as plant_code
;

select VIN
, manuf_name
, vehicle_type
, make_name
, plant_name
, model_year_name as model_year
, model_name
, desc1
, desc2
, desc3
, desc4
, desc5
, engine
, drive_type
, transmission
, mpg
from
  ( SELECT $sample_vin as VIN
  , LEFT($sample_vin,3) as WMI
  , SUBSTR($sample_vin,4,5) as VDS
  , SUBSTR($sample_vin,10,1) as model_year_code
  , SUBSTR($sample_vin,11,1) as plant_code
  ) vin
JOIN vin.decode.wmi_to_manuf w 
    ON vin.wmi = w.wmi
JOIN vin.decode.manuf_to_make m
    ON w.manuf_id=m.manuf_id
JOIN vin.decode.manuf_plants p
    ON vin.plant_code=p.plant_code
    AND m.make_id=p.make_id
JOIN vin.decode.model_year y
    ON vin.model_year_code=y.model_year_code
JOIN vin.decode.make_model_vds vds
    ON vds.vds=vin.vds 
    AND vds.make_id = m.make_id;

create or replace secure function vin.decode.parse_and_enhance_vin(this_vin varchar(25))
returns table (
    VIN varchar(25)
    , manuf_name varchar(25)
    , vehicle_type varchar(25)
    , make_name varchar(25)
    , plant_name varchar(25)
    , model_year varchar(25)
    , model_name varchar(25)
    , desc1 varchar(25)
    , desc2 varchar(25)
    , desc3 varchar(25)
    , desc4 varchar(25)
    , desc5 varchar(25)
    , engine varchar(25)
    , drive_type varchar(25)
    , transmission varchar(25)
    , mpg varchar(25)
)
as $$

 
select VIN
, manuf_name
, vehicle_type
, make_name
, plant_name
, model_year_name as model_year
, model_name
, desc1
, desc2
, desc3
, desc4
, desc5
, engine
, drive_type
, transmission
, mpg
from
  ( SELECT THIS_VIN as VIN
  , LEFT(THIS_VIN,3) as WMI
  , SUBSTR(THIS_VIN,4,5) as VDS
  , SUBSTR(THIS_VIN,10,1) as model_year_code
  , SUBSTR(THIS_VIN,11,1) as plant_code
  ) vin
JOIN vin.decode.wmi_to_manuf w 
    ON vin.wmi = w.wmi
JOIN vin.decode.manuf_to_make m
    ON w.manuf_id=m.manuf_id
JOIN vin.decode.manuf_plants p
    ON vin.plant_code=p.plant_code
    AND m.make_id=p.make_id
JOIN vin.decode.model_year y
    ON vin.model_year_code=y.model_year_code
JOIN vin.decode.make_model_vds vds
    ON vds.vds=vin.vds 
    AND vds.make_id = m.make_id

 
$$;

select *
from table(vin.decode.PARSE_AND_ENHANCE_VIN('SAJAJ4FX8LCP55916'));

select *
from table(vin.decode.PARSE_AND_ENHANCE_VIN('19UUB2F34LA001631'));

select *
from table(vin.decode.PARSE_AND_ENHANCE_VIN('19UUB2F34LA001631'));

