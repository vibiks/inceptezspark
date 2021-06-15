package sparksql

import org.apache.spark.sql.{SparkSession,Row}

import org.apache.spark.sql.types._

//import org.apache.spark.sql.Row

object Lab05_schema_df extends App 
{
  
    val spark = SparkSession.builder().appName("Lab03").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.filter(x => x.length == 5)
    
    val rdd3 = rdd2.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
    val custid = StructField("Custid",IntegerType,true)
    val fname = StructField("FName",StringType,true)
    val lname = StructField("LName",StringType,true)
    val age = StructField("Age",IntegerType,true)
    val prof =  StructField("Prof",StringType,true)
    val schema = StructType(List(custid,fname,lname,age,prof))
    
    /*val schema = StructType(List(StructField("Custid",IntegerType,true),
    StructField("FName",StringType,true),
    StructField("LName",StringType,true),
    StructField("Age",IntegerType,true),
    StructField("Prof",StringType,true)))*/
    
    val df = spark.createDataFrame(rdd3,schema)
    
    df.show()
    
    df.printSchema()
  
}
