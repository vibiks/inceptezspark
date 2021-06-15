package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab18_postgres_ops 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab17-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
     /*val df = spark.read.format("jdbc")
    .option("url","jdbc:postgresql://localhost/retail")
    .option("user","hduser")
    .option("password","hduser")
    .option("dbtable","customer")
    .option("driver","org.postgresql.Driver")
    .load()
    
    df.show()
    
    */
    
     val df1 = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","tblcustomer")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()
    
    
    df1.write.format("jdbc")
    .option("url","jdbc:postgresql://localhost/retail")
    .option("user","hduser")
    .option("password","hduser")
    .option("dbtable","tblcustomer")
    .option("driver","org.postgresql.Driver")
    .save()
    
    
    println("Data written into postgres from mysql")
    
  }
  
}