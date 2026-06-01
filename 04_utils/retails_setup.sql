-- show catalogs;
-- create catalog retails;

-- show schemas in retails;
-- create schema retails.bronze 
--   managed location 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/';
-- create schema retails.silver;
-- create schema retails.gold;

-- show grants on schema retails.bronze;
-- describe schema extended retails.bronze;

-- GRANT USE CATALOG ON CATALOG retails TO `dp-sales-engineers`;
-- GRANT USE SCHEMA ON CATALOG retails TO `dp-sales-engineers`;
-- GRANT CREATE TABLE ON SCHEMA retails.bronze TO `dp-sales-engineers`;
-- GRANT CREATE TABLE ON SCHEMA retails.silver TO `dp-sales-engineers`;
-- GRANT CREATE TABLE ON SCHEMA retails.gold TO `dp-sales-engineers`;

-- select * from retails.bronze.customers_raw;

-- GRANT MANAGE ON SCHEMA retails.bronze TO `dp-sales-engineers`;
-- GRANT MANAGE ON SCHEMA retails.silver TO `dp-sales-engineers`;
-- GRANT MANAGE ON SCHEMA retails.gold TO `dp-sales-engineers`;


-- GRANT MODIFY  ON SCHEMA retails.bronze TO `dp-sales-engineers`;
-- GRANT MODIFY  ON SCHEMA retails.silver TO `dp-sales-engineers`;
-- GRANT MODIFY  ON SCHEMA retails.gold TO `dp-sales-engineers`;

-- REVOKE MODIFY ON SCHEMA retails.bronze FROM `dp-sales-engineers`;
-- REVOKE MODIFY ON SCHEMA retails.silver FROM `dp-sales-engineers`;
-- REVOKE MODIFY ON SCHEMA retails.gold FROM `dp-sales-engineers`;

-- CREATE TABLE retails.bronze.customers_raw
-- USING DELTA
-- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/customers_raw';

-- CREATE TABLE retails.bronze.customers_raw
-- USING DELTA
-- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/customers_raw';

-- CREATE EXTERNAL VOLUME data.raw._schemas
-- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/_schemas';

-- CREATE EXTERNAL VOLUME data.raw._checkpoints
-- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/_checkpoints';

-- show volumes in data.raw;

-- GRANT READ VOLUME ON VOLUME data.raw._schemas TO `dp-sales-engineers`;
-- GRANT READ VOLUME ON VOLUME data.raw._checkpoints TO `dp-sales-engineers`;

-- GRANT WRITE VOLUME ON VOLUME data.raw._schemas TO `dp-sales-engineers`;
-- GRANT WRITE VOLUME ON VOLUME data.raw._checkpoints TO `dp-sales-engineers`;

-- SHOW GRANTS ON VOLUME data.raw._schemas;
-- SHOW GRANTS ON VOLUME data.raw._checkpoints

-- show grants on catalog data;
-- create volume data.raw.dev;
-- create volume data.raw.qa;
-- create volume data.raw.prod;

-- describe volume data.raw.sales;
-- SHOW GRANTS ON VOLUME data.raw.dev;

-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data.raw.dev TO `dp-sales-engineers`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data.raw.qa TO `dp-sales-engineers`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data.raw.prod TO `dp-sales-engineers`;

-- CREATE VOLUME data.raw.retail_db;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data.raw.retail_db TO `dp-sales-engineers`;

-- CREATE VOLUME data.raw.tpch;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data.raw.tpch TO `dp-sales-engineers`;

-- CREATE VOLUME data.raw.partitioned_data;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data.raw.partitioned_data TO `dp-sales-engineers`;

-- create schema retails.silver
-- managed location 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/silver'

-- GRANT CREATE TABLE, MANAGE, USE SCHEMA ON SCHEMA retails.silver TO `dp-sales-engineers`;


-- create schema retails.gold
-- managed location 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/gold'

-- GRANT CREATE TABLE, MANAGE, USE SCHEMA ON SCHEMA retails.gold TO `dp-sales-engineers`;


