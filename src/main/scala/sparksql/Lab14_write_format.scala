package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object Lab14_write_format {
  
   def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab14-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
     val df = spark.read.format("csv").option("inferschema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","prof")
    
    //select * from customer where age > 50 and prof is not null
    
    val df1 = df.filter("age > 50").where("prof is not null")
    
    //df1.show()
    
    //or 
    /*df.createOrReplaceTempView("customer")
    
    spark.sql("select * from customer where age > 50 and prof is not null").show()
		*/
    
    df1.repartition(5).write.format("csv")
    .option("header",true)
    //.option("delimiter","$")
    .mode(SaveMode.Append)
    //Append,Overwrite,ErrorIfexist,Ignore 
    //or
    //.mode("append")
    .save("file:/home/hduser/customercsv")
    //or
    df1.write.mode(SaveMode.Append).csv("file:/home/hduser/customerjson")
    
    
    df1.write.format("json").mode(SaveMode.Append).save("file:/home/hduser/customerjson")
    //or
    df1.write.mode(SaveMode.Append).json("file:/home/hduser/customerjson")
    
    
    df1.write.format("parquet").mode(SaveMode.Append).save("file:/home/hduser/customerparquet")
    //or
    df1.write.mode(SaveMode.Append).parquet("file:/home/hduser/customerparquet")
    
    
    df1.write.format("orc").mode(SaveMode.Append).save("file:/home/hduser/customerorc")
    //or
    df1.write.mode(SaveMode.Append).orc("file:/home/hduser/customerorc")
    
    df1.write.mode(SaveMode.Append).partitionBy("prof").csv("file:/home/hduser/customerpart")
    
    
    println("Written into the local filesystem")
       
  
  }
}