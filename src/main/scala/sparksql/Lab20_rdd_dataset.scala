package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

import org.apache.spark.sql.Dataset


object Lab20_rdd_dataset {
  
  case class customer(custid:Int,fname:String,lname:String,age:Int,prof:String)
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab19-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    val rdd = spark.sparkContext.textFile("file:/home/hduser/hive/data/custs")
    
    val rdd1 = rdd.map(x => x.split(",")).filter(x => x.length == 5)
    
    val rdd2 = rdd1.map(x => customer(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
    val ds:Dataset[customer] = spark.createDataset(rdd2)
    
    ds.printSchema()
    ds.show()
    
    val df = spark.createDataFrame(rdd2)
    
    df.printSchema
    
        
  }
}