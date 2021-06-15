package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab11_sqlquery {
  
   def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab10-SQL").master("local")
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","prof")
    
    //select * from customer where prof = 'Pilot' or prof = 'Teacher'
    //df.filter("prof = 'Pilot'").show()
    
    //or
    df.createOrReplaceTempView("customer")
    
    //val df1 = spark.sql("select * from customer where prof = 'Pilot'")
    
    //spark.sql("select * from customer where prof = 'Pilot' or prof = 'Teacher'").show()
    
    val df1 = spark.sql("select prof,count(*) as cnt from customer group by prof order by prof desc")
    
    val df2 = df1.filter("cnt > 100")
    
    df2.show()
    
    
    val df3 = df.groupBy("prof")
                .agg(count(col("custid"))
                .alias("cnt"))
                .orderBy(col("prof").desc)
                .filter("cnt > 100")
                .show()
    
    
    
    
    
       
    
    
    
    
    
  }
  
  
}