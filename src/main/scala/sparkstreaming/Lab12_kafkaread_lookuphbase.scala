package sparkstreaming
import org.apache.spark.SparkConf
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


object Lab12_kafkaread_lookuphbase 
{
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab12-Streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
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
              rdd.foreachPartition(part =>
               {
                 
                 val config = HBaseConfiguration.create()
	               val table = new HTable(config, "txns")
                 
                 part.foreach(row =>
                   {
                     val g = new Get(Bytes.toBytes(row));
                     
                     val result = table.get(g);
                     // read values from Result class object
            	      val txndate = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("txndate"));
            	      val txncustid = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("txncustid"));
            	      val amount = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("amount"));
            	      val category = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("category"));
            	      val product = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("product"));
            	      val  city = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("city"));
            	      val state = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("state"));
            	      val paymenttype = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("paymenttype"));
            	      
            	      val str  = row + "," + Bytes.toString(txndate) + "," + Bytes.toString(txncustid) + "," + Bytes.toString(amount) + "," + Bytes.toString(category) + "," + 
            	                    Bytes.toString(product) + "," + Bytes.toString(city) + "," + Bytes.toString(state) + "," + Bytes.toString(paymenttype) 
                   
            	      println(str)
                     
                   })                
                 
                 
               })
          }
       })
      
      //Apply transformation and manipulate the data
      
      
      
      
      
    
  
    
    ssc.start()
    ssc.awaitTermination()
  
  
  }
  
}