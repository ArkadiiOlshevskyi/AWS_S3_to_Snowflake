-- EXAMPLE HOW TO TRANSFER DATA FROMN S3 TO SNOWFLAKE


-- Show available databases
SHOW DATABASES;


-- Create the necessary warehouse, database, and schema if they do not already exist
CREATE WAREHOUSE IF NOT EXISTS S3_TO_SF_WH;
CREATE DATABASE IF NOT EXISTS S3_TO_SF_DB;
CREATE SCHEMA IF NOT EXISTS S3_TO_SF_DB.Prod;

-- Set the context for the role, warehouse, and database
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE S3_TO_SF_WH;
USE DATABASE S3_TO_SF_DB;


-- Creating integration object to connect to S3 Bucket
CREATE OR REPLACE STORAGE INTEGRATION Show_OBJ
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '$AWS_ROLE_ARN'
    STORAGE_ALLOWED_LOCATIONS = ('$AWS_ROLE_LINK');

    
-- Get data credentials for AWS policy JSON (optional)
DESC INTEGRATION Show_OBJ;


-- Create CSV format for data files
CREATE OR REPLACE FILE FORMAT csv_format 
    TYPE = CSV 
    FIELD_DELIMITER = ',' 
    SKIP_HEADER = 1 
    NULL_IF = ('NULL', 'null') 
    EMPTY_FIELD_AS_NULL = TRUE;

    
-- Creating a stage for loading data from S3
CREATE OR REPLACE STAGE show_stage_s3_sf
    STORAGE_INTEGRATION = Show_OBJ
    URL = '$S3_BUCKET_URL'
    FILE_FORMAT = csv_format;

    
-- Create a target table to hold the imported data
CREATE OR REPLACE TABLE employee_sales_s3 (
    Emp_ID INT,
    Emp_Name VARCHAR(50),
    SaleDate VARCHAR(20),
    SaleAmount FLOAT
);


-- Test connection to S3
LIST @show_stage_s3_sf;


-- Load data from S3 to Snowflake
COPY INTO employee_sales_s3 
FROM @show_stage_s3_sf 
ON_ERROR = 'continue';  -- or use 'skip_file'


-- Query to check the data loaded into Snowflake
SELECT * FROM employee_sales_s3 LIMIT 5;