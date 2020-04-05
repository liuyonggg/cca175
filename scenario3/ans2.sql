create external table orders_sqoop
STORED AS AVRO
LOCATION '/user/hive/warehouse/retail_stage.db/orders'
TBLPROPERTIES ('avro.schema.url'='/user/hive/schemas/order/orders.avsc')
