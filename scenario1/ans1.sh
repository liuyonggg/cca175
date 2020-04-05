sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--table orders \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--target-dir /user/cloudera/problem1/orders \
--as-avrodatafile;
