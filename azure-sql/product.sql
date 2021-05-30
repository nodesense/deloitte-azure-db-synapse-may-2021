CREATE TABLE products (
   id INT,
   name VARCHAR(256),
   sku VARCHAR(256),
   category_id INT,
   inventory_id INT, 
   price decimal,
   discount_id int,
   created_at datetime,
   modified_at datetime,
   deleted_at  datetime,
)


CREATE TABLE product_category (
   id INT,
   name VARCHAR(256),
   created_at datetime,
   modified_at datetime,
   deleted_at  datetime
)

CREATE TABLE product_discount (
   discount_id INT,
   name VARCHAR(256),
   discount_percent decimal,
   active 	int,
   created_at datetime,
   modified_at datetime,
   deleted_at  datetime
)


CREATE TABLE product_inventory (
   discount_id INT,
   quantity INT, 
   active 	int,
   created_at datetime,
   modified_at datetime,
   deleted_at  datetime
)



