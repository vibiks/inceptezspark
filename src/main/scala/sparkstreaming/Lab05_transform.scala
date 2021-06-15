package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Lab05_transform 
{
  
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab05-Streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    val dstream1 = ssc.socketTextStream("localhost", 9999)
    val dstream2 = dstream1.map(x => x.split(","))
    val dstream3 = dstream2.filter(x => x.length == 5)    
    val dstream4 = dstream3.map(x => (x(0).toInt,x(1),x(2).toInt,x(3).toFloat,x(4).toInt))
    
    //transform - converts dstream into rdd and returns back as dstream
    val dstream5 = dstream4.transform(rdd => rdd.sortBy(x => x._3,true))
    
    dstream5.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  
}