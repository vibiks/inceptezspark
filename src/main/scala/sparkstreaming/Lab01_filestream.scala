package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Lab01_filestream 
{
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab01-Streaming").setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    val dstream1 = ssc.textFileStream("file:/home/hduser/sparkstream")
    dstream1.print()    
    ssc.start()
    ssc.awaitTermination()
  }
  
}