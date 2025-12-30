-- Bật LOCAL INFILE
SET GLOBAL local_infile = 1;
SET SESSION local_infile = 1;

-- 1. distribution_centers
LOAD DATA LOCAL INFILE '/tmp/data/distribution_centers.csv'
INTO TABLE distribution_centers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, name, latitude, longitude);

-- 2. products
LOAD DATA LOCAL INFILE '/tmp/data/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, cost, category, name, brand, retail_price, department, sku, distribution_center_id);

-- 3. users
LOAD DATA LOCAL INFILE '/tmp/data/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, first_name, last_name, email, age, gender, state, street_address, postal_code, city, country,
 latitude, longitude, traffic_source, created_at);

-- 4. inventory_items
LOAD DATA LOCAL INFILE '/tmp/data/inventory_items.csv'
INTO TABLE inventory_items
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, product_id, created_at, sold_at, cost,
 product_category, product_name, product_brand, product_retail_price,
 product_department, product_sku, product_distribution_center_id);

-- 5. orders
LOAD DATA LOCAL INFILE '/tmp/data/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, user_id, status, gender, created_at, returned_at, shipped_at, delivered_at, num_of_item);

-- 6. order_items
LOAD DATA LOCAL INFILE '/tmp/data/order_items.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, order_id, user_id, product_id, inventory_item_id,
 status, created_at, shipped_at, delivered_at, returned_at, sale_price);

-- 7. events
LOAD DATA LOCAL INFILE '/tmp/data/events.csv'
INTO TABLE events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, user_id, session_id, sequence_number, created_at, ip_address,
 city, state, postal_code, browser, traffic_source, uri, event_type);
