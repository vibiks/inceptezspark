package sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Lab08_window {
  
  
   def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab08-Streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    ssc.checkpoint("file:/tmp/sparkcheckpoint")
    
    val dstream1 = ssc.socketTextStream("localhost", 9999)
    
    val dstream2 = dstream1.flatMap(x => x.split(" "))
    
    val dstream3 = dstream2.map(x => (x,1))
    
    val dstream4 = dstream3.window(Seconds(15),Seconds(10))
    
    dstream3.print()
    
    dstream4.print() 
    
    
    
    
    ssc.start()
    ssc.awaitTermination()
  }
  
}