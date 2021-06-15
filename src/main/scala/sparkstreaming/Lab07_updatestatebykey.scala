package sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object Lab07_updatestatebykey {
  
   def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab07-Streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    
    /*
  The checkpoint method defined in the StreamingContext class tells Spark Streaming to periodically
checkpoint data. 
It takes the name of a directory as an argument. 

For a production application, the checkpoint directory should be on a fault-tolerant 
storage system such as HDFS.
			
Data checkpointing is required when an application performs stateful transformation on a data stream. 
A stateful transformation is an operation that combines data across multiple batches in a data stream. 
An RDD generated by a stateful transformation depends on the previous batches in a data stream.
     */
    
    
    ssc.checkpoint("file:/tmp/sparkcheckpoint")
    
    val dstream1 = ssc.socketTextStream("localhost", 9999)
    
    val dstream2 = dstream1.flatMap(x => x.split(" "))
    
    val dstream3 = dstream2.map(x => (x,1))
    
    //val dstream4 = dstream3.reduceByKey((x,y) => x + y)
    
    val dstream4 = dstream3.updateStateByKey(updatestate)
    
    dstream4.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
   def updatestate(curval:Seq[Int],previousvalue:Option[Int]) =
    {
      previousvalue match
      {
        case Some(x) => Some(x + curval.sum)
        case None => Some(curval.sum)
      }
    } 
  
}