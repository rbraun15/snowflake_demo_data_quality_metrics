/*
Data Quality uses data metric functions (DMFs), which include Snowflake-provided system DMFs and user-defined DMFs, 
to monitor the state and integrity of your data. You can use DMFs to measure key metrics, such as, but not limited to, 
freshness and counts that measure duplicates, NULLs, rows, and unique values

https://docs.snowflake.com/en/user-guide/data-quality-intro

 */


--------------------------------------------------
-- Create DB, Schema, Table, Load data
--------------------------------------------------
 
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS dq_tutorial_db;
CREATE SCHEMA IF NOT EXISTS sch;

CREATE or replace TABLE customers (
  account_number NUMBER(38,0),
  first_name VARCHAR(16777216),
  last_name VARCHAR(16777216),
  email VARCHAR(16777216),
  phone VARCHAR(16777216),
  created_at TIMESTAMP_NTZ(9),
  street VARCHAR(16777216),
  city VARCHAR(16777216),
  state VARCHAR(16777216),
  country VARCHAR(16777216),
  zip_code VARCHAR(20),
  annual_spend number
);




INSERT INTO customers (account_number, city, country, email, first_name, last_name, phone, state, street, zip_code, annual_spend)
  VALUES (1589420, 'san francisco', 'usa', 'john.doe@', 'john', 'doe', 1234567890, null, null, null, 4322);

INSERT INTO customers (account_number, city, country, email, first_name, last_name, phone, state, street, zip_code, annual_spend)
  VALUES (1589450, 'san francisco', 'usa', 'jeff@bird', 'jeff', 'bird', 1234567890, null, null, null,4);

INSERT INTO customers (account_number, city, country, email, first_name, last_name, phone, state, street, zip_code, annual_spend)
  VALUES (1589450, 'san francisco', 'usa', 'jan.brady', 'jan', 'brady', 1234567890, null, null, null,4);

  
INSERT INTO customers (account_number, city, country, email, first_name, last_name, phone, state, street, zip_code, annual_spend)
  VALUES (8028387, 'san francisco', 'usa', 'bart.simpson@example.com', 'bart', 'simpson', 1012023030, null, 'market st', '94102',4343);

INSERT INTO customers (account_number, city, country, email, first_name, last_name, phone, state, street, zip_code, annual_spend)
  VALUES
    (1589420, 'san francisco', 'usa', 'john.doe@example.com', 'john', 'doe', 1234567890, 'ca', 'concar dr', '94402',7778),
    (2834123, 'san mateo', 'usa', 'jane.doe@example.com', 'jane', 'doe', 3641252911, 'ca', 'concar dr', '94402',765765),
    (4829381, 'san mateo', 'usa', 'jim.doe@example.com', 'jim', 'doe', 3641252912, 'ca', 'concar dr', '94402',35),
    (9821802, 'san francisco', 'usa', 'susan.smith@example.com', 'susan', 'smith', 1234567891, 'ca', 'geary st', '94121',475745),
    (8028387, 'san francisco', 'usa', 'bart.simpson@example.com', 'bart', 'simpson', 1012023030, 'ca', 'market st', '94102',6788);


INSERT INTO customers (account_number, city, country, email, first_name, last_name, phone, state, street, annual_spend)
  VALUES (8028387, 'san francisco', 'usa', 'brad.simpson@example.com', 'brad', 'simpson', 1012023030, null, 'market st',5435);
  
--------------------------------------------------
-- SYSTEM System Data Metric Funtions
-- Run statemetns ad hoc
--------------------------------------------------

-- NULL_COUNT - how many zip_codes are null?
SELECT SNOWFLAKE.CORE.NULL_COUNT( SELECT zip_code  FROM dq_tutorial_db.sch.customers );

-- NULL_PERCENT - what percent of zip_codes are null?
SELECT SNOWFLAKE.CORE.NULL_PERCENT( SELECT zip_code  FROM dq_tutorial_db.sch.customers );

-- AVG - what's the avg value?
SELECT SNOWFLAKE.CORE.AVG( SELECT annual_spend  FROM dq_tutorial_db.sch.customers );

-- MIN - what's the min value?
SELECT SNOWFLAKE.CORE.MIN( SELECT annual_spend  FROM dq_tutorial_db.sch.customers );

-- MAX - what's the max value?
SELECT SNOWFLAKE.CORE.MAX( SELECT annual_spend  FROM dq_tutorial_db.sch.customers );

 
----------------------------------------------------
-- Create a custom function to check for valid emails
----------------------------------------------------
    CREATE DATA METRIC FUNCTION IF NOT EXISTS
  invalid_email_count (ARG_T table(ARG_C1 STRING))
  RETURNS NUMBER AS
  'SELECT COUNT_IF(FALSE = (
    ARG_C1 REGEXP ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$''))
    FROM ARG_T';

    
---------------------------------------------------- 
-- Test the function we just created
----------------------------------------------------
SELECT  invalid_email_count( SELECT email  FROM dq_tutorial_db.sch.customers );


---------------------------------------------------- 
-- Add the data metric schedule to our table    
---------------------------------------------------- 
-- every 5 minutes
-- ALTER TABLE customers SET DATA_METRIC_SCHEDULE = '5 MINUTE';
-- evrery 8 hours
ALTER TABLE customers SET DATA_METRIC_SCHEDULE   = 'USING CRON 0 */8 * * * UTC';
-- stop scheduled run
ALTER TABLE customers UNSET DATA_METRIC_SCHEDULE;

 

----------------------------------------------------
-- Add individual metrics to the table, 
-- they will run at above schedueld interval
----------------------------------------------------
ALTER TABLE customers ADD DATA METRIC FUNCTION
  invalid_email_count ON (email);

ALTER TABLE customers ADD DATA METRIC FUNCTION
  SNOWFLAKE.CORE.NULL_COUNT on (zip_code);

-- can not run this statement ad hoc, must be added to a table
ALTER TABLE customers ADD DATA METRIC FUNCTION
  SNOWFLAKE.CORE.ROW_COUNT on ();

  
----------------------------------------------------------------
-- See the metrics defined on my table and see schedule status
----------------------------------------------------------------
  SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
  REF_ENTITY_NAME => 'dq_tutorial_db.sch.customers',
  REF_ENTITY_DOMAIN => 'TABLE'));

  
----------------------------------------------------
-- See metric values
----------------------------------------------------
SELECT scheduled_time, measurement_time, table_name, metric_name, value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE TRUE
-- AND METRIC_NAME = 'INVALID_EMAIL_COUNT'
AND TABLE_DATABASE = 'DQ_TUTORIAL_DB'
order by measurement_time desc
LIMIT 100;



---------------------------------------------------------------
-- See failing metric values
-- Only supports built in functions at this time, not custom
---------------------------------------------------------------
SELECT *
  FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME  => 'customers',
    METRIC_NAME  => 'SNOWFLAKE.CORE.NULL_COUNT',
    ARGUMENT_NAME => 'zip_code'
  ));



----------------------------------------------------
-- See credit usage
----------------------------------------------------
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_USAGE_HISTORY
WHERE TRUE
AND START_TIME >= CURRENT_TIMESTAMP - INTERVAL '3 days'
LIMIT 100;


----------------------------------------------------
-- Clean up
----------------------------------------------------
-- stop scheduled run
ALTER TABLE customers UNSET DATA_METRIC_SCHEDULE; 

-- Drop database
CREATE DATABASE IF NOT EXISTS dq_tutorial_db;
