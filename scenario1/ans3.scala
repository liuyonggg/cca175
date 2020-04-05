import com.databricks.spark.avro._;

var ordersDF = sqlContext.read.avro("/user/cloudera/problem1/orders")
var orderItemDF = sqlContext.read.avro("/user/cloudera/problem1/order-items")


