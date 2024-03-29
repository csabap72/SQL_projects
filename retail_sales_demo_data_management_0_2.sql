-- demo sql scripts for the retail sales data analysis
-- data can be found at: https://www.kaggle.com/datasets/manjeetsingh/retaildataset?resource=download
-- version 0.2 23/02/2024
-- DISCLAIMER: this is a demo script, no guarantee whatsoever is provided as for its usability, correctness, effectiveness, best practice etc. You are using/studying/applying it entirely at your own risk (please see licence)

---------------------------------------------------------
-- Creating schemas
---------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS retail_sales;

----------------------------------------------------------
-- Creating the import tables
----------------------------------------------------------

DROP TABLE IF EXISTS retail_sales.sales_data_import;

CREATE TABLE retail_sales.sales_data_import
(
store integer NOT NULL,
department integer NOT NULL,
date varchar(10) NOT NULL,
weekly_sales numeric(10,2) NOT NULL,
is_holiday character(5) NOT NULL,
upload_timestamp TIMESTAMP NULL
);

DROP TABLE IF EXISTS retail_sales.store_data_import;

CREATE TABLE retail_sales.store_data_import
(
store integer NOT NULL,
type character(1) NOT NULL,
size integer NOT NULL,
upload_timestamp TIMESTAMP NULL
);

DROP TABLE IF EXISTS retail_sales.features_data_import;

CREATE TABLE retail_sales.features_data_import
(
store integer  NOT NULL,
date varchar(10) NOT NULL,
temperature numeric (6,2) NOT NULL, 
fuel_price numeric(5,3) NOT NULL,
markdown_1 varchar(20) NOT NULL,
markdown_2 varchar(20) NOT NULL,
markdown_3 varchar(20) NOT NULL,
markdown_4 varchar(20) NOT NULL,
markdown_5 varchar(20) NOT NULL,
cpi varchar(20) NOT NULL,
unemployment varchar(20) NOT NULL,
is_holiday character(5) NOT NULL,
upload_timestamp TIMESTAMP NULL
);

-----------------------------------------------------------------
-- data upload into the _import tables
-----------------------------------------------------------------

-- retail_sales.features_data_import 

DELETE FROM retail_sales.features_data_import;

-- to be run from pgAdmin PSQLTool
--\COPY retail_sales.features_data_import (store, date, temperature, fuel_price, markdown_1, markdown_2, markdown_3, markdown_4, markdown_5, cpi, unemployment, is_holiday) FROM '/home/csaba/Documents/data_import/CSV_upload/Features data set.csv' WITH DELIMITER ',' CSV HEADER;

SELECT COUNT(*) FROM retail_sales.features_data_import;

UPDATE retail_sales.features_data_import
SET
	upload_timestamp = NOW()
WHERE
	upload_timestamp IS NULL;
	
	
-- retail_sales.sales_data_import	

DELETE FROM retail_sales.sales_data_import;

-- to be run from pgAdmin PSQLTool
-- \COPY retail_sales.sales_data_import (store, department, date, weekly_sales, is_holiday) FROM '/home/csaba/Documents/data_import/CSV_upload/sales data-set.csv' WITH DELIMITER ',' CSV HEADER;

SELECT COUNT(*) FROM retail_sales.sales_data_import;

UPDATE retail_sales.sales_data_import
SET
	upload_timestamp = NOW()
WHERE
	upload_timestamp IS NULL;
	

-- retail_sales.sales_data_import

DELETE FROM retail_sales.store_data_import;

-- to be run from pgAdmin PSQLTool
-- \COPY retail_sales.store_data_import (store, type, size) FROM '/home/csaba/Documents/data_import/CSV_upload/stores data-set.csv' WITH DELIMITER ',' CSV HEADER;

UPDATE retail_sales.store_data_import
SET
	upload_timestamp = NOW()
WHERE
	upload_timestamp IS NULL;

SELECT COUNT(*) FROM retail_sales.store_data_import;

-- very simple upload validation (because of the NOT NULL constraint all rows should contain value, row numbers are enough)

WITH rnb_features_data_import AS
    (SELECT 'row_number' AS data_type, COUNT(*) AS row_number FROM retail_sales.features_data_import)
   , rnb_sales_data_import AS
    (SELECT 'row_number' AS data_type, COUNT(*) AS row_number FROM retail_sales.sales_data_import)
   , rnb_store_data_import AS
    (SELECT 'row_number' AS data_type, COUNT(*) AS row_number FROM retail_sales.store_data_import)
SELECT rnb_features_data_import.row_number AS features_rnb_import, rnb_sales_data_import.row_number AS sales_data_rnb_import, rnb_store_data_import.row_number AS store_data_rnb_import
FROM rnb_features_data_import JOIN rnb_sales_data_import ON rnb_features_data_import.data_type = rnb_sales_data_import.data_type
                            JOIN rnb_store_data_import ON rnb_features_data_import.data_type = rnb_store_data_import.data_type;

-----------------------------------------------------
-- live tables for the data analysis
-----------------------------------------------------

DROP TABLE IF EXISTS retail_sales.sales_data_live;

CREATE TABLE retail_sales.sales_data_live
(
store integer NOT NULL,
department integer NOT NULL,
date date NOT NULL,
weekly_sales numeric(10,2) NOT NULL,
is_holiday character(5) NOT NULL,
upload_timestamp TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS retail_sales.store_data_live;

CREATE TABLE retail_sales.store_data_live
(
store integer NOT NULL,
type character(1) NOT NULL,
size integer NOT NULL,
upload_timestamp TIMESTAMP NOT NULL
);


DROP TABLE IF EXISTS retail_sales.features_data_live;

CREATE TABLE retail_sales.features_data_live
(
store integer  NOT NULL,
date date NOT NULL,
temperature numeric (6,2) NOT NULL, 
fuel_price numeric(5,3) NOT NULL,
markdown_1 numeric(10,2) NULL,
markdown_2 numeric(10,2) NULL,
markdown_3 numeric(10,2) NULL,
markdown_4 numeric(10,2) NULL,
markdown_5 numeric(10,2) NULL,
cpi numeric(10,7) NULL,
unemployment numeric(15,7) NULL,
is_holiday character(5) NOT NULL,
upload_timestamp TIMESTAMP
);

-- inserting the data into the live tables with some exception handling (eg NA values) and date conversion

-- retail_sales.features_data_live

INSERT INTO retail_sales.features_data_live
SELECT
fdi.store,
CASE WHEN LENGTH (fdi.date)=8 THEN TO_DATE(fdi.date, 'DD/MM/YY') ELSE TO_DATE(fdi.date, 'DD/MM/YYYY') END AS date,
fdi.temperature,
fdi.fuel_price,
CAST (CASE WHEN fdi.markdown_1='NA' THEN NULL ELSE fdi.markdown_1 END AS NUMERIC(10,2)) AS markdown_1,
CAST (CASE WHEN fdi.markdown_2='NA' THEN NULL ELSE fdi.markdown_2 END AS NUMERIC(10,2)) AS markdown_2,
CAST (CASE WHEN fdi.markdown_3='NA' THEN NULL ELSE fdi.markdown_3 END AS NUMERIC(10,2)) AS markdown_3,
CAST (CASE WHEN fdi.markdown_4='NA' THEN NULL ELSE fdi.markdown_4 END AS NUMERIC(10,2)) AS markdown_4,
CAST (CASE WHEN fdi.markdown_5='NA' THEN NULL ELSE fdi.markdown_5 END AS NUMERIC(10,2)) AS markdown_5,
CAST (CASE WHEN fdi.cpi='NA' THEN NULL ELSE fdi.cpi END AS NUMERIC(10,7)) AS cpi,
CAST (CASE WHEN fdi.unemployment='NA' THEN NULL ELSE fdi.unemployment END AS NUMERIC(10,7)) AS unemployment,
fdi.is_holiday,
fdi.upload_timestamp
FROM retail_sales.features_data_import AS fdi 
WHERE fdi.upload_timestamp NOT IN (SELECT upload_timestamp FROM retail_sales.features_data_live);


-- retail_sales.sales_data_live

INSERT INTO retail_sales.sales_data_live
SELECT
sdi.store,
sdi.department,
CASE WHEN LENGTH (sdi.date)=8 THEN TO_DATE(sdi.date, 'DD/MM/YY') ELSE TO_DATE(sdi.date, 'DD/MM/YYYY') END AS date,
sdi.weekly_sales,
sdi.is_holiday,
sdi.upload_timestamp
FROM retail_sales.sales_data_import AS sdi 
WHERE sdi.upload_timestamp NOT IN (SELECT upload_timestamp FROM retail_sales.sales_data_live);


-- retail_sales.store_data_live

INSERT INTO retail_sales.store_data_live
SELECT
store,
type,
size,
upload_timestamp
FROM retail_sales.store_data_import
WHERE upload_timestamp NOT IN (SELECT upload_timestamp FROM retail_sales.store_data_live);

-- very simple upload validation (because of the NOT NULL constraint all rows should contain value, row numbers are enough)

WITH rnb_features_data_live AS
(SELECT 'row_number' AS data_type, COUNT(*) AS row_number FROM retail_sales.features_data_live)
, rnb_sales_data_live AS
(SELECT 'row_number' AS data_type, COUNT(*) AS row_number FROM retail_sales.sales_data_live)
, rnb_store_data_live AS
(SELECT 'row_number' AS data_type, COUNT(*) AS row_number FROM retail_sales.store_data_live)
SELECT rnb_features_data_live.row_number AS features_rnb, rnb_sales_data_live.row_number AS sales_data_rnb, rnb_store_data_live.row_number AS store_data_rnb
FROM rnb_features_data_live JOIN rnb_sales_data_live ON rnb_features_data_live.data_type = rnb_sales_data_live.data_type
JOIN rnb_store_data_live ON rnb_features_data_live.data_type = rnb_store_data_live.data_type;