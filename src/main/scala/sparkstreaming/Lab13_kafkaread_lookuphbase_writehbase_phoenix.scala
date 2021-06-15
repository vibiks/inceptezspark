package sparkstreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


//kafka packages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


//HBase packages
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


object Lab13_kafkaread_lookuphbase_writehbase_phoenix
{
  def main(args:Array[String])=
  {
    
    val spark = SparkSession.builder.appName("Lab02-Streaming").master("local[*]").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
     val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "test1"
          )
    
          
      val topics = Array("transtopic")
      
     
      val dstream1 = KafkaUtils.createDirectStream[String, String](ssc,LocationStrategies.PreferBrokers,Subscribe[String, String](topics, kafkaParams))
      
      val dstream2 = dstream1.map(x => x.value())
      
      dstream2.foreachRDD(rdd =>
        {
          if(!rdd.isEmpty())
          {
            
              //lookup with hbase table for every row in the rdd
              val rdd1 = rdd.mapPartitions(part =>
               {
                 
                 val config = HBaseConfiguration.create()
	               val table = new HTable(config, "txns")
                 
                   part.map(row =>
                   {
                     val g = new Get(Bytes.toBytes(row));
                    
                     //Read from hbase by passing transid as parameter
                    val result = table.get(g);
                    val txndate = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("txndate"));
            	      val txncustid = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("txncustid"));
            	      val amount = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("amount"));
            	      val category = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("category"));
            	      val product = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("product"));
            	      val  city = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("city"));
            	      val state = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("state"));
            	      val paymenttype = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("paymenttype"));
            	      
            	      (row , Bytes.toString(txndate) , Bytes.toString(txncustid), Bytes.toString(amount) , Bytes.toString(category) , Bytes.toString(product), Bytes.toString(city) , Bytes.toString(state) , Bytes.toString(paymenttype)) 
                      
                   })
               })
                   //convert rdd to dataframe
                   import spark.implicits._                   
                   val df = rdd1.toDF("txnid","txndate","custid","amt","category","product","city","state","paytype")
                   
                   //writing into phoenix                   
                   df.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", "TRANSDATA").option("zkUrl", "localhost:2181").save()
                   
                   println("Written into HBase")
          }
       })
      
      
      
      
    
  
    
    ssc.start()
    ssc.awaitTermination()
  
  
  }
  
}