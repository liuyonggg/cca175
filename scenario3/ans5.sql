-- 29s
set hive.exec.dynamic.partition.mode=non-strict;
drop database if exists retail cascade; 
drop table if exists orders_avro;

create database retail;

create table orders_avro 
(order_id int, 
order_date date,
order_customer_id int,
order_status string)
partitioned by (order_month string)
STORED AS AVRO;

insert overwrite table orders_avro partition (order_month)
select order_id, to_date(from_unixtime(cast(order_date/1000 as int))), order_customer_id, order_status, substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month from default.orders_sqoop;



