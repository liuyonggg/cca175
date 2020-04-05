-- took 81s
select * from orders_sqoop as X where X.order_date in (select inner.order_date from (select Y.order_date, count(1) as total_orders from orders_sqoop as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) inner);
