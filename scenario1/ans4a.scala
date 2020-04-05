import com.databricks.spark.avro._;

var ordersDF = sqlContext.read.avro("/user/cloudera/problem1/orders")
var orderItemDF = sqlContext.read.avro("/user/cloudera/problem1/order-items")

var joinedOrderDataDF = ordersDF.join(orderItemDF, ordersDF("order_id") === orderItemDF("order_item_order_id"))

var res = joinedOrderDataDF.
groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_formatted_date"),col("order_status")).
agg(round(sum("order_item_subtotal"),2).alias("total_amount"),countDistinct("order_id").alias("total_orders")).
orderBy(col("order_formatted_date").desc,col("order_status"),col("total_amount").desc,col("total_orders"));

res.show();

var dataFrameResult = joinedOrderDataDF

joinedOrderDataDF.registerTempTable("order_joined");

var sqlResult = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders from order_joined group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status order by order_formatted_date desc,order_status,total_amount desc, total_orders");

sqlResult.show();

var comByKeyResult = 
joinedOrderDataDF.
map(x=> ((x(1).toString,x(3).toString),(x(8).toString.toFloat,x(0).toString))).
combineByKey((x:(Float, String))=>(x._1,Set(x._2)),
(x:(Float,Set[String]),y:(Float,String))=>(x._1 + y._1,x._2+y._2),
(x:(Float,Set[String]),y:(Float,Set[String]))=>(x._1+y._1,x._2++y._2)).
map(x=> (x._1._1,x._1._2,x._2._1,x._2._2.size)).
toDF().
orderBy(col("_1").desc,col("_2"),col("_3").desc,col("_4"));

comByKeyResult.show();

sqlContext.setConf("spark.sql.parquet.compression.codec","gzip");
dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-gzip");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-gzip");
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-gzip");

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");
dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-snappy");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-snappy");
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-snappy");

dataFrameResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4a-csv")
sqlResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4b-csv")
comByKeyResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4c-csv")
