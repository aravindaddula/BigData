/*
SELECT *
FROM employees

insert into employees (emp_id,emp_name,department,salary) values
(11,'Newton','Sales',71000.00) ,
(12,'Judith','Mrketing',68000.00) ,
(13,'Judith','Mrketing',69000.00)
*/

with get_postgres_types as (
select 
    table_catalog,table_schema,table_name,concat('col("',column_name,'")') as column_name ,data_type as postgres_data_type
    ,case when data_type in ('character varying','text') then 'StringType'
         when data_type in ('bigint','integer') then 'IntegerType'
         when data_type in ('timestamp without time zone') then 'TimestampType'
         when data_type in ('date') then 'DateType'
         when data_type in ('numeric') then 'DecimalType'
     end as spark_data_type
from information_schema.columns
where table_schema in ('public')
)
select 
     table_catalog,table_schema,table_name,string_agg(column_name,',') as all_columns
    ,string_agg(postgres_data_type,',') as postgres_data_types
    ,string_agg(spark_data_type,',') as spark_data_types
from get_postgres_types
group by table_catalog,table_schema,table_name
