create table departments (
department_id int,
department_name string
) row format delimited fields terminated by ',';

create table categories (
category_id int,
category_department_id int,
category_name string
) row format delimited fields terminated by ',';

create table products (
product_id int,
product_category_id int,
product_name string,
product_description string,
product_price float,
product_image string
) row format delimited fields terminated by ',';

create table order_items (
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
) row format delimited fields terminated by ',';

create table orders (
order_id int,
order_date string,
order_customer_id int,
order_status string
) row format delimited fields terminated by ',';

create table customers (
customer_id int,
customer_fname string,
customer_lname string,
customer_email string,
customer_password string,
customer_street string,
customer_city string,
customer_state string,
customer_zipcode string
) row format delimited fields terminated by ',';

load data inpath 'datasets/retail_db/departments' into table departments;
load data inpath 'datasets/retail_db/categories' into table categories;
load data inpath 'datasets/retail_db/products' into table products;
load data inpath 'datasets/retail_db/order_items' into table order_items;
load data inpath 'datasets/retail_db/orders' into table orders;
load data inpath 'datasets/retail_db/customers' into table customers;
