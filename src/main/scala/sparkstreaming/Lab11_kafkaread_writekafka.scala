package sparkstreaming
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


//kafka packages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//kafka prducer
import org.apache.kafka.clients.producer._

object Lab11_kafkaread_writekafka 
{
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab11-Streaming").setMaster("local[*]")
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
      
      val dstreamdata = dstream1.map(x => x.value())
      
      val dstream2 = dstreamdata.map(x => x.split(","))
    
      val dstream3 = dstream2.filter(x => x.length == 9) 
      
      def convertarraytojson(ar:Array[String])=
      {
         s"""{"txnid":"${ar(0)}","txndate":"${ar(1)}","custid":"${ar(2)}","amt":"${ar(3)}","category":"${ar(4)}","product":"${ar(5)}","city":"${ar(6)}","state":"${ar(7)}","ptype":"${ar(8)}"}"""
      }
      
      val dstream4 = dstream3.map(convertarraytojson)
      
      dstream4.foreachRDD(rdd =>
        {
          if(!rdd.isEmpty())
          {
              rdd.foreachPartition(part =>
               {
                  val props = new Properties()
                  props.put("bootstrap.servers", "localhost:9092")
                  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                  val producer = new KafkaProducer[String, String](props)
                  
                  part.foreach(row =>
                    {
                      //create message
                      val record = new ProducerRecord[String, String]("tk7", null, row)
                      
                      //send message
                      producer.send(record)
                      println("Written into kafka topic")
                      
                    })
              })
          }
          
        })
      
      
  
    
    ssc.start()
    ssc.awaitTermination()
  
  
  }
  
}