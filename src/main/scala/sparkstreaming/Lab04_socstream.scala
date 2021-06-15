package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp


object Lab04_socstream 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("Lab03-Streaming").master("local[2]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    //To install netcat for socket
    //sudo yum install nc.x86_64
    val dstream1 = ssc.socketTextStream("localhost", 9999)
    //dstream1.print()
    
    val dstream2 = dstream1.map(x => x.split(","))
    
    val dstream3 = dstream2.filter(x => x.length == 5)
    
    val dstream4 = dstream3.map(x => (x(0).toInt,x(1),x(2).toInt,x(3).toFloat,x(4).toInt))
    
    
    dstream4.foreachRDD(rdd =>
    {
      if(!rdd.isEmpty())
      {
          import spark.implicits._
          val df = rdd.toDF("movieid","moviename","releaseyear","movierating","movieduration")
          
          val df1 = df.withColumn("created_dt", current_timestamp())
          
          df1.write.format("jdbc")
          .mode("append")
          .option("url","jdbc:postgresql://localhost/retail")
          .option("user","hduser")
          .option("password","hduser")
          .option("dbtable","tblmovie")
          .option("driver","org.postgresql.Driver")
          .save()
         
          println("Written into postgres")
      }
      
    })    
    
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}