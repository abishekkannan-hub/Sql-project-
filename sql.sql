use sales_records;

create table if not exists sales_main_table (
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
    product_id int,
    product_name Varchar(150),
    category Varchar(100),
    brand Varchar(100),
    product_price Decimal(10,2),
    quantity int,
    item_price decimal(10,2),
    payment_id int,
    payment_date date,
    amount decimal(10,2),
    payment_method Varchar(50));
 
    Create table customers_final ( 
    customer_id int,	
    first_name Varchar(100),
    last_name Varchar(100),	
    email Varchar(100),
    phone bigint,	
    address Varchar(100));
    
    Create table products_final(
    product_id int,
    product_name varchar(100),
    category varchar(20),
    brand varchar(20),	
    product_price decimal(10,2));
 
   Create table stores_final(
   store_id int,
   store_name varchar(50),	
   store_city varchar(50),	
   store_state varchar(50),
   store_manager varchar(50));

    Create table orders_final(
    order_id int,
    customer_id int,
    store_id int,
    order_date date,
    status Varchar(50),
    shipping_cost decimal(10,2),
    discount decimal(10,2));
 
    Create table order_items_final(
	order_id int,
    product_id int,
    quantity int,
    item_price decimal(10,2));
    
  Create table payments_final(
    payment_id int,
    order_id int,
    payment_date date,
    amount decimal(10,2),
    payment_method varchar(50));  
    
 
delimiter $$
create procedure sales_data()
begin    

truncate table customer_backup;
select * into customer_backup from customers;

delete from customers
where customer_id in (
  select customer_id from (
    select 
      customer_id,row_number() over(partition by first_name, last_name ,email ,phone ,address
	order by customer_id) as rn
    from customers
  ) as ranked
  where rn > 1);

insert into customers_final (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address)
select distinct
    cast(customer_id as signed) as customer_id,
    coalesce(trim(first_name), last_name) as first_name,
    coalesce(trim(last_name), first_name) as last_name,
    coalesce(trim(email), 'no mail registered') as email,
    cast(coalesce(nullif(REGEXP_REPLACE(phone, '[^0-9]', ''), ''), '0000000000') as unsigned) as phone,
    coalesce(trim(address), 'unknown') as address
from customers_final;

 truncate table stores_backup;
select * into stores_backup from stores;

delete from store_id
where store_id in (
  select store_id from (
    select
      store_id,row_number() over (partition by store_name, store_city, store_state, store_manager
        order by store_id) as rn
    from stores
  ) as ranked
  where rn > 1);

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

 truncate table products_backup;
select * into products_backup from productss;

delete from products
where product_id in (
  select product_id from (
    select
      product_id,row_number() over (partition by product_name, category, brand, price
        order by product_id) as rn
    from products
  ) as ranked
  where rn > 1);

insert into products_final (
    product_id, 
    product_name, 
    category, 
    brand, 
    product_price)
select distinct
    cast(product_id as signed) as product_id,
    coalesce(trim(product_name), 'unknown') as product_name,
    coalesce(trim(category), 'unknown') as category,
    coalesce(trim(brand), 'unknown') as brand,
    coalesce(cast(nullif(REGEXP_REPLACE(price, '[^0-9\.]', ''), '') as decimal(10,2)),0.00) as product_price
from products;

 truncate table orders_backup;
select * into orders_backup from orders;

delete from orders
where order_id in (
  select order_id from (
    select
      order_id,row_number() over (partition by customer_id, store_id, order_date, status, shipping_cost, discount
        order by order_id) as rn
    from order_id
  ) as ranked
  where rn > 1);

insert into orders_final (
    order_id,
    customer_id,
    store_id,
    order_date,
    status,
    shipping_cost,
    discount)
    
select distinct
    cast(customer_id as signed) as customer_id,
    cast(order_id as signed) as order_id,
    cast(store_id as signed) as store_id,
    coalesce(str_to_date(nullif(order_date, ''), '%Y-%m-%d'),date('1900-01-01')) as order_date,
    coalesce(trim(status), 'unknown') as status,
    coalesce(cast(nullif(REGEXP_REPLACE(shipping_cost, '[^0-9\.]', ''), '') as decimal(10,2)),0.00) as shipping_cost,
    coalesce(cast(nullif(REGEXP_REPLACE(discount, '[^0-9\.]', ''), '') as decimal(10,2)),0.00) as discount
from orders;

 
 truncate table order_item_backup;
select * into order_item_backup from customers;
	
delete from order_item_id
where order_item_id in (
  select order_item_id from (
    select 
      order_item_id,row_number() over (partition by order_id, product_id, quantity, price
	order by product_id) as rn
    from order_item_id
  ) as ranked
  where rn > 1);

   insert into order_items_final (
    order_id,
    product_id,
    quantity,
    item_price)
select distinct
    cast(order_id as signed) as order_id,
    cast(product_id as signed) as product_id,
    coalesce(cast(nullif(REGEXP_REPLACE(quantity, '[^0-9\.]', ''), '') as decimal(10,2)),0) as quantity,
    coalesce(cast(nullif(REGEXP_REPLACE(price, '[^0-9\.]', ''), '') as decimal(10,2)),0.00) as item_price
from order_items; 
 
truncate table payments_backup;
select * into payments_backup from payments;

delete from payment_id
where payment_id in (
  select payment_id from (
    select 
      payment_id,row_number() over (partition by order_id, payment_date, amount, payment_method
        order by payment_id) as rn
    from payment_id
  ) as ranked
  where rn > 1);

        
    insert into payments_final (
    payment_id,
    order_id,
    payment_date,
    amount,
    payment_method)
select distinct
    cast(payment_id as signed) as payment_id,
    cast(order_id as signed) as order_id,
	coalesce(str_to_date(nullif(payment_date, ''), '%Y-%m-%d'),date('1900-01-01')) as payment_date,
    coalesce(cast(nullif(REGEXP_REPLACE(amount, '[^0-9\.]', ''), '') as decimal(10,2)),0.00) as amount,
    coalesce(trim(payment_method), 'unknown') as payment_method
from payments;

end $$

delimiter ;

delimiter $$
create procedure sales_data_main()
begin   

truncate table sales_main_table_backup;
select * into sales_main_table_backup from sales_main_table;
   
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

select
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
left join customers_final c on o.customer_id = c.customer_id
left join stores_final s on o.store_id = s.store_id
left join order_items_final oi on o.order_id = oi.order_id
left join  products_final p on oi.product_id = p.product_id
left join payments_final pm on o.order_id = pm.order_id;

end $$

delimiter ;

call sales_data();
call sales_data_main();







