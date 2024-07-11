--copy CSV data to mysql container
    --docker cp brazilian-ecommerce/ de_mysql:/tmp/
    --docker cp mysql_schemas.sql de_mysql:/tmp/

--login to mysql server as root
    --docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}
    --SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';
    --SET GLOBAL LOCAL_INFILE=TRUE;

--docker exec -it de_mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}


--source /tmp/mysql_scripts.sql

-- Load Data for Sellers Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_sellers_dataset.csv'
INTO TABLE olist_sellers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Load Data for Products Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_products_dataset.csv'
INTO TABLE olist_products_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Load Data for Order Items Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Load Data for Customers Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ','  -- Assuming the fields are comma-separated
ENCLOSED BY '"'           -- Assuming the fields are enclosed by double quotes
LINES TERMINATED BY '\n'  -- Assuming each line is a new record
IGNORE 1 LINES;            -- Ignore the header row

-- Load Data for Order Payments Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Load Data for Orders Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Load Data for Product Category Name Translation Table
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

select * from olist_customers_dataset limit 10;
select * from olist_order_payments_dataset limit 10;
select * from olist_products_dataset limit 10;
select * from olist_sellers_dataset limit 10;
select * from olist_order_items_dataset limit 10;
select * from olist_orders_dataset limit 10;
select * from product_category_name_translation limit 10;

--truncate olist_order_items_dataset;
--truncate olist_products_dataset;
--
--truncate olist_customers_dataset;
--truncate olist_order_payments_dataset;
--truncate olist_sellers_dataset;
--truncate olist_orders_dataset;
--truncate product_category_name_translation;
