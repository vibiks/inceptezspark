package structurestreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Lab03_file
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab01-structstream").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val studschema = StructType(List(
        StructField("studid",IntegerType,true),
        StructField("studname",StringType,true),
        StructField("m1",IntegerType,true),
        StructField("m2",IntegerType,true),
        StructField("m3",IntegerType,true)
        )) 
    val df = spark.readStream.format("csv").schema(studschema).option("header", false).option("path","file:/home/hduser/sparkstream").load()
    
    
    val df1 = df.withColumn("current_dt", current_date())
    
    val df2 = df1.withColumn("TotalMarks", col("m1") + col("m2") + col("m3") )
    
    df2.writeStream.format("console").start().awaitTermination()
    
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
