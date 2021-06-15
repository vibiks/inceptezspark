package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._


object Lab17_write_jdbc 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab17-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs").toDF("custid","fname","lname","age","prof")
  
    df.write.format("jdbc").option("url","jdbc:mysql://localhost/custdb")
    .mode("overwrite")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","tblcustomer")
    .option("driver","com.mysql.cj.jdbc.Driver").save()
    
    
    
    
    
  }
  
  
}