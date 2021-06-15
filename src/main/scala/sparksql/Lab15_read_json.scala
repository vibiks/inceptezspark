package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object Lab15_read_json {
  
   def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab15-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
     //val df = spark.read.format("json").option("inferschema",true).load("file:/home/hduser/customercsv/*.json")
     //or
     val df = spark.read.option("inferschema",true).parquet("file:/home/hduser/customercsv/*.json")
     df.show()
         
     df.filter("prof='Pilot'").show()
     
    val df1 = spark.read.option("inferschema",true).option("multiline","true").json("file:/home/hduser/empdetails.json")
    
    df.printSchema()
    
    //Access from struct type
    df1.select("address.city","address.state","address.zipcode","empid","empname").show() 
    
    //or
    df1.createOrReplaceTempView("customerjson")
    spark.sql("select address.city,address.state,address.zipcode,empid,empname from customerjson").show()
    
    //Access from array
    df1.select(col("address.city"),col("skills")(0).alias("primaryskill"),col("skills")(1).alias("secondaryskill")).show()
    //or
    spark.sql("select address.city,skills[0] as primaryskill,skills[1] as secondaryskill from customerjson").show()
    
    df1.select(col("empid"),col("empname"),col("designation"),explode(col("skills")).alias("skill"),col("address")).show()
  
  }
}