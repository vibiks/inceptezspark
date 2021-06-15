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


object Lab09_kafkaread 
{
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab08-Streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
     val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "test1"
          )
    
          
      val topics = Array("transtopic")
      
      /*In most cases, you should use LocationStrategies.PreferConsistent. 
				This will distribute partitions evenly across available executors. 

				If your executors are on the same hosts as your Kafka brokers, use PreferBrokers, 
				which will prefer to schedule partitions on the Kafka leader for that partition. 
				
				Finally, if you have a significant skew in load among partitions, use PreferFixed. 
				This allows you to specify an explicit mapping of partitions to hosts (any unspecified partitions will use a consistent location).
      */
      val dstream1 = KafkaUtils.createDirectStream[String, String](ssc,LocationStrategies.PreferBrokers,Subscribe[String, String](topics, kafkaParams))
      
      val dstream2 = dstream1.map(x => x.value())
      
      
      //Apply transformation and manipulate the data
      
      
      
      
      dstream2.print()
      
    
  
    
    ssc.start()
    ssc.awaitTermination()
  
  
  }
  
}