USE ecommerce;

CREATE OR REPLACE SCHEMA clean;

-- Creating and transforming distribution_centers table
CREATE OR REPLACE TABLE clean.distribution_centers 
AS
SELECT * FROM raw.distribution_centers;

-- Splitting Name column into State and City and ordering by ID
CREATE OR REPLACE TABLE clean.distribution_centers 
AS
SELECT ID,
LEFT(NAME,LEN(NAME) - CHARINDEX(' ',REVERSE(NAME))) AS CITY,
RIGHT(NAME,CHARINDEX(' ',REVERSE(NAME))-1) AS STATE,
LATITUDE,
LONGITUDE,
DISTRIBUTION_CENTER_GEOM
FROM clean.distribution_centers 
ORDER BY ID;

-- Creating events table with necessary transformations
CREATE OR REPLACE TABLE clean.events (
id Number(18,0),
user_id Number(9,0),
sequence_number Number(9,0),
sequence_id STRING,
created_at TIMESTAMP,
ip_address  STRING,
city STRING,
state STRING,
postal_code STRING,
browser STRING,
traffic_source STRING,
uri STRING,
event_type STRING
);

BEGIN;

TRUNCATE TABLE clean.events;

INSERT INTO clean.events
WITH processed_events AS (
    SELECT 
        id,
        user_id,
        sequence_number,
        sequence_id,
        TRY_TO_TIMESTAMP(
            LEFT(created_at, LEN(created_at) - POSITION(' ', REVERSE(created_at)))
        ) AS created_at,
        ip_address,
        city,
        state,
        postal_code,
        browser,
        traffic_source,
        uri,
        event_type
    FROM raw.events
)
SELECT * FROM processed_events;

COMMIT;

ALTER TABLE clean.events CLUSTER BY (state);

-- Creating and inserting data into inventory_items
CREATE OR REPLACE TABLE clean.inventory_items(
id Number(18,0),
product_id Number(9,0),
created_at TIMESTAMP,
sold_at TIMESTAMP,
cost Number(36,14),
product_category STRING,
product_name STRING,
product_brand STRING,
product_retail_price NUMBER(36,14),
product_department STRING,
product_sku STRING,
product_distribution_center_id NUMBER(9,0)
);

BEGIN;

INSERT INTO clean.inventory_items
WITH processed_inventory_items AS (
SELECT
id,
product_id,
TRY_TO_TIMESTAMP(
    LEFT(created_at,LENGTH(created_at)-POSITION(' ',REVERSE(created_at)))
) AS created_at,
TRY_TO_TIMESTAMP(
    LEFT(sold_at, LENGTH(sold_at)-POSITION(' ',sold_at))
) AS sold_at,
cost,
product_category,
product_name,
product_brand,
product_retail_price,
product_department,
product_sku,
product_distribution_center_id
FROM raw.inventory_items
)
SELECT * FROM processed_inventory_items;

COMMIT;

-- Creating and inserting data into order_items
CREATE OR REPLACE TABLE clean.order_items(
id Number(18,0),
order_id Number(18,0),
user_id Number(18,0),
product_id Number(18,0),
inventory_item_id Number(18,0),
status STRING,
created_at TIMESTAMP,
shipped_at TIMESTAMP,
delivered_at TIMESTAMP,
returned_at TIMESTAMP,
sale_price NUMBER(36,20)
);

BEGIN;

INSERT INTO clean.order_items
WITH processed_order_items AS (
SELECT
id,
order_id,
user_id,
product_id,
inventory_item_id,
status,
TRY_TO_TIMESTAMP(
    LEFT(created_at,LENGTH(created_at)-POSITION(' ',REVERSE(created_at)))
) AS created_at,
TRY_TO_TIMESTAMP(
    LEFT(shipped_at,LENGTH(shipped_at)-POSITION(' ',REVERSE(shipped_at)))
) AS shipped_at,
TRY_TO_TIMESTAMP(
    LEFT(delivered_at,LENGTH(delivered_at)-POSITION(' ',REVERSE(delivered_at)))
) AS delivered_at,
TRY_TO_TIMESTAMP(
    LEFT(returned_at,LENGTH(returned_at)-POSITION(' ',REVERSE(returned_at)))
) AS returned_at,
sale_price
FROM raw.order_items
)
SELECT * FROM processed_order_items;

COMMIT;

-- Creating and inserting data into orders
CREATE OR REPLACE TABLE clean.orders(
order_id Number(9,0),
user_id Number(9,0),
status STRING,
gender STRING,
created_at TIMESTAMP,
returned_at TIMESTAMP,
shipped_at TIMESTAMP,
delivered_at TIMESTAMP,
num_of_item NUMBER(9,0)
);

BEGIN;

INSERT INTO clean.orders
WITH processed_orders AS (
SELECT
order_id,
user_id,
status,
gender,
TRY_TO_TIMESTAMP(
    LEFT(created_at,LENGTH(created_at)-POSITION(' ',REVERSE(created_at)))
) AS created_at,
TRY_TO_TIMESTAMP(
    LEFT(returned_at,LENGTH(returned_at)-POSITION(' ',REVERSE(returned_at)))
) AS returned_at,
TRY_TO_TIMESTAMP(
    LEFT(shipped_at,LENGTH(shipped_at)-POSITION(' ',REVERSE(shipped_at)))
) AS shipped_at,
TRY_TO_TIMESTAMP(
    LEFT(delivered_at,LENGTH(delivered_at)-POSITION(' ',REVERSE(delivered_at)))
) AS delivered_at,
num_of_item
FROM raw.orders
)
SELECT * FROM processed_orders;

COMMIT;

-- Creating clean products table
CREATE OR REPLACE TABLE clean.products
AS
SELECT * FROM ecommerce.raw.products;

-- Creating and inserting data into users table
CREATE OR REPLACE TABLE clean.users(
  id NUMBER(18,0),             
  first_name STRING,          
  last_name STRING,            
  email STRING,                
  age NUMBER(3,0),            
  gender STRING,              
  state STRING,                
  street_address STRING,       
  postal_code STRING,          
  city STRING,                 
  country STRING,              
  latitude NUMBER(9,4),      
  longitude NUMBER(9,4),      
  traffic_source STRING,      
  created_at TIMESTAMP,           
  user_geom GEOGRAPHY          
);

BEGIN;

INSERT INTO clean.users
WITH processed_users AS (
SELECT
id,
first_name,
last_name,
email,
age,
gender,
state,
street_address,
postal_code,
city,
country,
latitude,
longitude,
traffic_source,
TRY_TO_TIMESTAMP(
    LEFT(created_at,LENGTH(created_at)-POSITION(' ',REVERSE(created_at)))
) AS created_at,
user_geom
FROM raw.users
)
SELECT * FROM processed_users;

COMMIT;
SELECT * FROM clean.users;

