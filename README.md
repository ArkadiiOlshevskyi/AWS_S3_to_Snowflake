# Snowflake Data Engineering Project

![How It Works](https://github.com/ArkadiiOlshevskyi/AWS_S3_to_Snowflake/blob/main/HowItWorks.jpg)

## Overview

This project demonstrates the ingestion and processing of data in Snowflake using Amazon S3 as the external storage source. The workflow involves pulling external data (CSV/Excel) from S3, loading it into Snowflake staging tables, and processing it into a new database and schema. The data is then transformed and analyzed into structured tables for further usage.

---

## Project Workflow

### 1. External Data Sources

- **Input Data**: 
  - CSV and Excel files stored on Amazon S3.
- **Data Types**:
  - History Day Data
  - Postal Code Data

### 2. Snowflake Setup Steps

- **Role Creation**:
  - A new role is created for accessing S3.
  - Access policies are assigned to this role to communicate with S3 and Snowflake.

- **Warehouse Setup**:
  - A new Snowflake warehouse is created to process and store the incoming data.
  
- **Database & Schema Setup**:
  - A new database and schema are created to organize and store the processed data.

- **Stage Creation**:
  - A stage is set up to temporarily store incoming data from external sources.

### 3. Data Ingestion from Amazon S3

- **Storage Integration**:
  - Storage integration objects are created in Snowflake to access data from the S3 bucket.
  
- **Credentials Management**:
  - Credentials from Snowflake are added to Amazon S3 policy settings.

- **S3 to Snowflake Connection**:
  - A LIST request is sent from Snowflake to S3 to ensure a connection is established.
  
- **Data Download**:
  - Data from the S3 bucket is downloaded into Snowflake for processing.

### 4. Data Loading & Processing (Processing DAGs)

- **Staging Table** (`FROSTBYTE_RAW_STAGE`):
  - The raw data is ingested into this staging area for further processing.

- **Processing Tasks**:
  - **LOAD_ORDER_DETAIL_TASK**: Loads detailed order data from the staging table to the order details table.
  - **LOAD_LOCATION_TASK**: Processes and loads location-related data.
  - **LOAD_DAILY_METRICS_SP_TASK**: Loads and processes daily metrics for various locations.

### 5. Processed Data (New Database / New Schema)

- **Processed Tables**:
  - **Location**: Contains processed data for various geographical locations.
  - **Order Detail**: Stores detailed information on orders after processing.
  - **Daily City Metrics**: Contains city-wise daily metrics based on location and order data.

### 6. Data Output

The processed data is available in Snowflake in structured tables, which are ready for downstream analysis or export.

---

## Prerequisites

1. **Snowflake Account**: Ensure you have a valid Snowflake account with administrative access.
2. **Amazon S3**: The CSV and Excel files should be uploaded to an accessible S3 bucket.
3. **Credentials**: S3 access keys and Snowflake credentials should be configured properly for seamless communication.

---

## Steps to Run

1. **Create Role in S3**:
   - Create an IAM role in AWS with policies that grant access to your S3 bucket.

2. **Configure S3 Role in Snowflake**:
   - Add Snowflake credentials to S3 IAM policies for secure data access.

3. **Snowflake Setup**:
   - Set up a warehouse, database, sc
