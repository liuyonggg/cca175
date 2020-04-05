sqoop import-all-tables \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--warehouse-dir /user/hive/warehouse/retail_stage.db \
--compress \
--compression-codec snappy \
--as-avrodatafile \
-m 1
hadoop fs -ls /user/hive/warehouse/retail_stage.db
