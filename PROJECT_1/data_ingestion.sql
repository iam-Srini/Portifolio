-- Create a new database named 'ecommerce' or replace it if it already exists
CREATE OR REPLACE DATABASE ecommerce;

-- Switch to the 'ecommerce' database
USE DATABASE ecommerce;

-- Create a new schema named 'land' to store landing (raw) data
CREATE OR REPLACE SCHEMA land;

-- Create a storage integration to connect Snowflake with Google Cloud Storage (GCS)
CREATE OR REPLACE STORAGE INTEGRATION gcs_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://your-bucket-name');

-- Describe the storage integration to view its details (e.g., GCS bucket permissions)
DESC STORAGE INTEGRATION gcs_int;

-- Create an external stage named 'my_gcs_stage' to point to the GCS bucket
CREATE OR REPLACE STAGE ecommerce.land.my_gcs_stage
  URL = 'gcs://your-bucket-name'
  STORAGE_INTEGRATION = gcs_int
  FILE_FORMAT = (TYPE = 'CSV');

-- List files in the GCS bucket to verify the connection
LIST @land.my_gcs_stage;

-- Create a new schema named 'raw' to store processed data
CREATE OR REPLACE SCHEMA raw;

-- Create a table to store distribution center data
CREATE OR REPLACE TABLE ecommerce.raw.distribution_centers(
  id NUMBER(9,0),              -- Unique identifier for the distribution center
  name STRING,                 -- Name of the distribution center
  latitude NUMBER(9,4),        -- Latitude coordinate
  longitude NUMBER(9,4),       -- Longitude coordinate
  distribution_center_geom GEOGRAPHY  -- Geographical location (latitude and longitude combined)
);

-- Create a table to store event data
CREATE OR REPLACE TABLE ecommerce.raw.events(
  id NUMBER(18,0),             -- Unique identifier for the event
  user_id NUMBER(9,0),         -- ID of the user associated with the event
  sequence_number NUMBER(9,0), -- Sequence number of the event
  sequence_id STRING,          -- Sequence ID for grouping related events
  created_at STRING,           -- Loaded as String when the event was created
  ip_address STRING,           -- IP address of the user
  city STRING,                 -- City of the user
  state STRING,                -- State of the user
  postal_code STRING,          -- Postal code of the user
  browser STRING,              -- Browser used by the user
  traffic_source STRING,       -- Source of traffic (e.g., organic, paid)
  uri STRING,                  -- URI of the event
  event_type STRING            -- Type of event (e.g., click, purchase)
);

-- Create a table to store inventory item data
CREATE OR REPLACE TABLE ecommerce.raw.inventory_items(
  id NUMBER(18,0),             -- Unique identifier for the inventory item
  product_id NUMBER(9,0),      -- ID of the product
  created_at STRING,           -- Loaded as String when the item was created
  sold_at STRING,              -- Loaded as String when the item was sold
  cost NUMBER(36,14),          -- Cost of the item
  product_category STRING,     -- Category of the product
  product_name STRING,         -- Name of the product
  product_brand STRING,        -- Brand of the product
  product_retail_price NUMBER(36,14),  -- Retail price of the product
  product_department STRING,   -- Department of the product
  product_sku STRING,          -- SKU of the product
  product_distribution_center_id NUMBER(9,0)  -- ID of the distribution center
);

-- Create a table to store order item data
CREATE OR REPLACE TABLE ecommerce.raw.order_items(
  id NUMBER(18,0),             -- Unique identifier for the order item
  order_id NUMBER(18,0),       -- ID of the order
  user_id NUMBER(18,0),        -- ID of the user
  product_id NUMBER(18,0),     -- ID of the product
  inventory_item_id NUMBER(18,0),  -- ID of the inventory item
  status STRING,               -- Status of the order item (e.g., shipped, delivered)
  created_at STRING,           -- Loaded as String when the item was created
  shipped_at STRING,           -- Loaded as String when the item was shipped
  delivered_at STRING,         -- Loaded as String when the item was delivered
  returned_at STRING,          -- Loaded as String when the item was returned
  sale_price NUMBER(36,20)     -- Sale price of the item
);

-- Create a table to store order data
CREATE OR REPLACE TABLE ecommerce.raw.orders(
  order_id NUMBER(9,0),        -- Unique identifier for the order
  user_id NUMBER(9,0),         -- ID of the user
  status STRING,               -- Status of the order (e.g., completed, returned)
  gender STRING,               -- Gender of the user
  created_at STRING,           -- Loaded as String when the order was created
  returned_at STRING,          -- Loaded as String when the order was returned
  shipped_at STRING,           -- Loaded as String when the order was shipped
  delivered_at STRING,         -- Loaded as String when the order was delivered
  num_of_item NUMBER(9,0)      -- Number of items in the order
);

-- Create a table to store product data
CREATE OR REPLACE TABLE ecommerce.raw.products(
  id NUMBER(18,0),             -- Unique identifier for the product
  cost NUMBER(36,20),          -- Cost of the product
  category STRING,             -- Category of the product
  name STRING,                 -- Name of the product
  brand STRING,                -- Brand of the product
  retail_price NUMBER(36,20),  -- Retail price of the product
  department STRING,           -- Department of the product
  sku STRING,                  -- SKU of the product
  distribution_center_id NUMBER(9,0)  -- ID of the distribution center
);

-- Create a table to store user data
CREATE OR REPLACE TABLE ecommerce.raw.users(
  id NUMBER(18,0),             -- Unique identifier for the user
  first_name STRING,           -- First name of the user
  last_name STRING,            -- Last name of the user
  email STRING,                -- Email address of the user
  age NUMBER(3,0),             -- Age of the user
  gender STRING,               -- Gender of the user
  state STRING,                -- State of the user
  street_address STRING,       -- Street address of the user
  postal_code STRING,          -- Postal code of the user
  city STRING,                 -- City of the user
  country STRING,              -- Country of the user
  latitude NUMBER(9,4),        -- Latitude coordinate of the user's location
  longitude NUMBER(9,4),       -- Longitude coordinate of the user's location
  traffic_source STRING,       -- Source of traffic (e.g., organic, paid)
  created_at STRING,           -- Loaded as String when the user account was created
  user_geom GEOGRAPHY          -- Geographical location (latitude and longitude combined)
);

-- Load data into the 'distribution_centers' table from the GCS bucket
COPY INTO raw.distribution_centers
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
FILES = ('distribution_centers.csv');

-- Load data into the 'events' table from the GCS bucket using a file pattern
COPY INTO ecommerce.raw.events
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN = '.*events\.csv';

-- Load data into the 'inventory_items' table from the GCS bucket using a file pattern
COPY INTO raw.inventory_items
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN = '.*inventory_items\.csv';

-- Load data into the 'order_items' table from the GCS bucket using a file pattern
COPY INTO raw.order_items
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN = '.*order_items\.csv';

-- Load data into the 'orders' table from the GCS bucket using a file pattern
COPY INTO raw.orders
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN = '.*orders.csv';

-- Load data into the 'products' table from the GCS bucket using a file pattern
COPY INTO raw.products
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN = '.*products.csv';

-- Load data into the 'users' table from the GCS bucket
COPY INTO raw.users
FROM @land.my_gcs_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
FILES = ('users.csv');