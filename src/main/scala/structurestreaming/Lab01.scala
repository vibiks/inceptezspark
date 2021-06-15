package structurestreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

object Lab01 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab01-structstream").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
   
    val df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    
    val df1 = df.withColumn("current_dt", current_date())
    
    df1.writeStream.format("console").start().awaitTermination()
    
  }
}

/*
  
 Read from Input sources -> Transformation -> Write into Sink 
 
Input Sources:
=============

Rate (for Testing): It will automatically generate data including 2 columns timestamp and value . This is generally used for testing purposes. 
Socket: This data source will listen to the specified socket and ingest any data into Spark Streaming.
File: This will listen to a particular directory as streaming data. It supports file formats like CSV, JSON, ORC, and Parquet.
Kafka: This will read data from Apache Kafka® and is compatible with Kafka broker versions 0.10.0 or higher 


Sink Types:
========== 

Console sink: Displays the content of the DataFrame to console
File sink: Stores the contents of a DataFrame in a file within a directory. Supported file formats are csv, json, orc, and parquet.
Kafka sink: Publishes data to a Kafka topic
Foreachbatch: allows you to specify a function that is executed on the output data of every micro-batch of the streaming query.
  
*/
