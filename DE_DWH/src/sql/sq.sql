/*  Наполнения витрины вознаграждения курьеров */ 

with ff as (select dd.courier_id,
courier_name,
year as settlement_year,
month as settlement_month,
order_id,
sum,
rate,
tip_sum,
(case when rate < 4 then sum*0.05
when rate >= 4 and rate < 4.5 then sum*0.07 
when rate >= 4.5 and rate <= 4.9 then sum*0.8
when rate > 4.9 then (sum*0.1) end) as money,
(case when rate < 4 then 100
when rate >= 4 and rate < 4.5 then 150 
when rate >= 4.5 and rate <= 4.9 then 175
when rate > 4.9 then 200 end) as money_2
from dds.dm_deliveries dd 
inner join dds.dm_couriers dc on dc.id = dd.courier_id 
inner join dds.dm_timestamps dt on dt.id  = dd.timestamp_id),
bb as (select *,
(case when money > money_2 then money else money_2 end) as money_3 from ff) 
select 
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	count(order_id) as orders_count,
	sum("sum") as order_total_sum,
	avg(rate) as rate_avg,
	(sum("sum"))*0.25 as order_processing_fee,
	sum(tip_sum) as courier_tips_sum,
	(sum(tip_sum) + sum(money_3))*0.95 as courier_reward_sum
from bb
group by 1,2,3,4


/*  Схема CDM */

CREATE TABLE cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id int4 NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	restaurant_reward_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_settlement_report_restaurant_id_settlement_date UNIQUE (restaurant_id, settlement_year, settlement_month, settlement_date)
);

CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg numeric(14, 2) NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	courier_order_sum numeric(14, 2) NOT NULL,
	courier_tips_sum numeric(14, 2) NOT NULL,
	courier_reward_sum numeric(14, 2) NOT NULL
);

/*  Схема DDS */

CREATE TABLE dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id)
);

CREATE TABLE dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NULL,
	"date" date NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pk PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

CREATE TABLE dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NULL,
	restaurant_name varchar NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE dds.dm_products (
	id serial4 NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pk PRIMARY KEY (id),
	CONSTRAINT product_price_check CHECK ((product_price >= (0)::numeric))
);
ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);

CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	CONSTRAINT dm_orders_kp PRIMARY KEY (id)
);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_restaurants_id FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_timestamps_id FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_users_id FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);

CREATE TABLE dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pk PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric))
);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_id FOREIGN KEY (product_id) REFERENCES dds.dm_products(id);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_order_id FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);

CREATE TABLE dds.dm_couriers (
	id int4 NOT NULL DEFAULT nextval('dds.couriers_id_seq'::regclass),
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id int4 NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	timestamp_id int4 NOT NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id)
);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_users_timestamp_id FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);

/* Схема STG */

CREATE TABLE stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.ordersystem_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT restaurants_pkey PRIMARY KEY (id)
);
CREATE TABLE stg.couriers (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT couriers_object_id_uindex UNIQUE (object_id),
	CONSTRAINT couriers_pkey PRIMARY KEY (id)
);
CREATE TABLE stg.deliveries (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts varchar NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	address varchar NOT NULL,
	delivery_ts varchar NOT NULL,
	rate int4 NOT NULL,
	sum int4 NOT NULL,
	tip_sum int4 NOT NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id)
);
