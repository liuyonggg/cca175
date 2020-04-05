sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--table order_items \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--target-dir /user/cloudera/problem1/order-items \
--as-avrodatafile;
