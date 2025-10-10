
use sales_records;

CREATE TABLE if not exists sales_main_table (
	sales_id int auto_increment primary key,
    order_id int,
    order_date date,
    status Varchar(50),
    shipping_cost Decimal(10,2),
    discount Decimal(10,2),
    customer_id int,
    first_name Varchar(100),
    last_name Varchar(100),
    email Varchar(150),
    phone bigint,
    address text,
    store_id int,
    store_name Varchar(150),
    store_city Varchar(100),
    store_state Varchar(100),
    store_manager Varchar(100),
    product_id INT,
    product_name Varchar(150),
    category Varchar(100),
    brand Varchar(100),
    product_price Decimal(10,2),
    quantity int,
    item_price Decimal(10,2),
    payment_id int,
    payment_date Date,
    amount Decimal(10,2),
    payment_method Varchar(50));
    
delimiter $$
create procedure sales_data()
begin    
	
  CREATE TABLE IF NOT EXISTS customers_backup LIKE customers;
  TRUNCATE TABLE customers_backup;
  INSERT INTO customers_backup SELECT * FROM customers;

  CREATE TABLE IF NOT EXISTS stores_backup LIKE stores;
  TRUNCATE TABLE stores_backup;
  INSERT INTO stores_backup SELECT * FROM stores;
  
  CREATE TABLE IF NOT EXISTS products_backup LIKE products;
  TRUNCATE TABLE products_backup;
  INSERT INTO products_backup SELECT * FROM products;
  
  CREATE TABLE IF NOT EXISTS orders_backup LIKE orders;
  TRUNCATE TABLE orders_backup;
  INSERT INTO orders_backup SELECT * FROM orders;
  
  CREATE TABLE IF NOT EXISTS order_items_backup LIKE order_items;
  TRUNCATE TABLE order_items_backup;
  INSERT INTO order_items_backup SELECT * FROM order_items;
  
  CREATE TABLE IF NOT EXISTS payments_backup LIKE payments;
  TRUNCATE TABLE payments_backup;
  INSERT INTO payments_backup SELECT * FROM payments;
  

 
DELETE c FROM customers c
JOIN  (SELECT customer_id,ROW_NUMBER() OVER (PARTITION BY first_name, last_name, email, phone, address ORDER BY customer_id) AS rn
FROM customers) t ON c.customer_id = t.customer_id WHERE t.rn > 1;

DELETE s FROM stores s
JOIN (SELECT store_id,ROW_NUMBER() OVER (PARTITION BY store_name, store_city, store_state, store_manager
ORDER BY store_id) AS rn FROM stores) t ON s.store_id = t.store_id WHERE t.rn > 1;

DELETE p FROM products p
JOIN (SELECT product_id,ROW_NUMBER() OVER (PARTITION BY product_name, category, brand, price
ORDER BY product_id) AS rn FROM products) t ON p.product_id = t.product_id WHERE t.rn > 1;
   
DELETE o FROM orders o
JOIN (SELECT order_id,ROW_NUMBER() OVER (PARTITION BY customer_id, store_id, order_date, status, shipping_cost, discount
ORDER BY order_id) AS rn FROM orders) t ON o.order_id = t.order_id WHERE t.rn > 1;

DELETE oi FROM order_items oi
JOIN (SELECT order_id, product_id, quantity, price,ROW_NUMBER() OVER (
PARTITION BY order_id, product_id, quantity, price ORDER BY order_id) AS rn
FROM order_items) t ON oi.order_id = t.order_id
AND oi.product_id = t.product_id
AND oi.quantity = t.quantity
AND oi.price = t.price
WHERE t.rn > 1;

DELETE pm FROM payments pm
JOIN (SELECT payment_id,ROW_NUMBER() OVER (PARTITION BY order_id, payment_date, amount, payment_method
ORDER BY payment_id) AS rn FROM payments) t ON pm.payment_id = t.payment_id WHERE t.rn > 1;


   Create table if not exists customers_final ( 
    customer_id int,	
    first_name Varchar(100),
    last_name Varchar(100),	
    email Varchar(100),
    phone bigint,	
    address Varchar(100));
        
INSERT INTO customers_final (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address)
SELECT distinct
    CAST(customer_id AS SIGNED) AS customer_id,
    COALESCE(TRIM(first_name), '') AS first_name,
    COALESCE(TRIM(last_name), '') AS last_name,
    COALESCE(TRIM(email), 'no mail registered') AS email,
    CAST(COALESCE(NULLIF(REGEXP_REPLACE(phone, '[^0-9]', ''), ''), '0000000000') AS UNSIGNED) AS phone,
    COALESCE(TRIM(address), 'unknown') AS address
FROM customers;
   
   
Create table if not exists products_final(
product_id int,
product_name varchar(100),
category varchar(20),
brand varchar(20),	
product_price decimal(10,2));
    
INSERT INTO products_final (
    product_id, 
    product_name, 
    category, 
    brand, 
    product_price)
SELECT distinct
    CAST(product_id AS SIGNED) AS product_id,
    COALESCE(TRIM(product_name), 'unknown') AS product_name,
    COALESCE(TRIM(category), 'unknown') AS category,
    COALESCE(TRIM(brand), 'unknown') AS brand,
    COALESCE(CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9\.]', ''), '') AS DECIMAL(10,2)),0.00) AS product_price
FROM products;
 
     
Create table if not exists stores_final(
store_id int,
store_name varchar(50),	
store_city varchar(50),	
store_state varchar(50),
store_manager varchar(50));
    
insert into stores_final(
store_id,
store_name,
store_city,
store_state,	
store_manager)
select distinct
     cast(store_id as signed)as store_id,
     coalesce(trim(store_name),'unknown')as store_name,
     coalesce(trim(store_city),'unknown')as store_city,
	 coalesce(trim(store_state),'unknown')as store_state,
     coalesce(trim(store_manager),'unknown')as store_manager
from stores;

 Create table if not exists orders_final(
    order_id int,
    customer_id int,
    store_id int,
    order_date date,
    status Varchar(50),
    shipping_cost Decimal(10,2),
    discount Decimal(10,2));
    
     
INSERT INTO orders_final (
    order_id,
    customer_id,
    store_id,
    order_date,
    status,
    shipping_cost,
    discount)
    
SELECT distinct
    CAST(customer_id AS SIGNED) AS customer_id,
    CAST(order_id AS SIGNED) AS order_id,
    CAST(store_id AS SIGNED) AS store_id,
    COALESCE(STR_TO_DATE(NULLIF(order_date, ''), '%Y-%m-%d'),DATE('1900-01-01')) AS order_date,
    COALESCE(TRIM(status), 'unknown') AS status,
    COALESCE(CAST(NULLIF(REGEXP_REPLACE(shipping_cost, '[^0-9\.]', ''), '') AS DECIMAL(10,2)),0.00) AS shipping_cost,
    COALESCE(CAST(NULLIF(REGEXP_REPLACE(discount, '[^0-9\.]', ''), '') AS DECIMAL(10,2)),0.00) AS discount
FROM orders;


Create table if not exists order_items_final(
	order_id int,
    product_id int,
    quantity int,
    item_price Decimal(10,2));
        
    INSERT INTO order_items_final (
    order_id,
    product_id,
    quantity,
    item_price
)
SELECT distinct
    CAST(order_id AS SIGNED) AS order_id,
    CAST(product_id AS SIGNED) AS product_id,
    COALESCE(CAST(NULLIF(REGEXP_REPLACE(quantity, '[^0-9\.]', ''), '') AS DECIMAL(10,2)),0) AS quantity,
    COALESCE(CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9\.]', ''), '') AS DECIMAL(10,2)),0.00) AS item_price
FROM order_items;

   
Create table if not exists payments_final(
    payment_id int,
    order_id int,
    payment_date Date,
    amount Decimal(10,2),
    payment_method Varchar(50));
     
    INSERT INTO payments_final (
    payment_id,
    order_id,
    payment_date,
    amount,
    payment_method
)
SELECT distinct
    CAST(payment_id AS SIGNED) AS payment_id,
    CAST(order_id AS SIGNED) AS order_id,
    COALESCE(STR_TO_DATE(NULLIF(payment_date, ''), '%Y-%m-%d'),DATE('1900-01-01')) AS payment_date,
    COALESCE(CAST(NULLIF(REGEXP_REPLACE(amount, '[^0-9\.]', ''), '') AS DECIMAL(10,2)),0.00) AS amount,
    COALESCE(TRIM(payment_method), 'unknown') AS payment_method
FROM payments;

DELETE FROM sales_main_table
WHERE sales_id IN (SELECT sales_id FROM (SELECT sales_id,ROW_NUMBER() OVER (
PARTITION BY order_id, product_id, quantity, item_price ORDER BY sales_id) AS rn FROM sales_main_table) t WHERE rn > 1);


   TRUNCATE sales_main_table;
insert into sales_main_table (
	
    order_id,
    order_date,
    status,
    shipping_cost,
    discount,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    store_id,
    store_name,
    store_city,
    store_state,
    store_manager,
    product_id,
    product_name,
    category,
    brand,
    product_price,
    quantity,
    item_price,
    payment_id,
    payment_date,
    amount,
    payment_method)
SELECT 

    o.order_id,
    o.order_date,
    o.status,
    o.shipping_cost,
    o.discount,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address,
    s.store_id,
    s.store_name,
    s.store_city,
    s.store_state,
    s.store_manager,
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.product_price,
    oi.quantity,
    oi.item_price,
    pm.payment_id,
    pm.payment_date,
    pm.amount,
    pm.payment_method
FROM orders_final o
LEFT JOIN customers_final c ON o.customer_id = c.customer_id
LEFT JOIN stores_final s ON o.store_id = s.store_id
LEFT JOIN order_items_final oi ON o.order_id = oi.order_id
LEFT JOIN products_final p ON oi.product_id = p.product_id
LEFT JOIN payments_final pm ON o.order_id = pm.order_id;

end $$

delimiter ;

call sales_data()



  





