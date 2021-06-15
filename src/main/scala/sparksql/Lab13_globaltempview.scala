package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab13_globaltempview {
  
   def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab13-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
     val df = spark.read.format("csv").option("inferschema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","prof")
    
    
    df.createOrReplaceGlobalTempView("tblcustomer")
    
    df.createOrReplaceTempView("tblcustomer")
    
    spark.sql("select custid,fname from global_temp.tblcustomer limit 10").show()
    
    val spark1 = spark.newSession()
    
    spark1.sql("""select custid,fname from 
                global_temp.tblcustomer limit 10""").show()
    
    spark.sql("select * from tblcustomer where prof='Pilot' limit 5").show()
    //spark1.sql("select * from tblcustomer where prof='Pilot' limit 5").show()
    
    spark.catalog.listTables("default").show()
    spark1.catalog.listTables("default").show()
    df.createOrReplaceGlobalTempView("tblcustomer")
    
    spark.catalog.listTables("global_temp").show()
    spark1.catalog.listTables("global_temp").show()
    
    spark.sql("select * from global_temp.tblcustomer where prof='Pilot' limit 5").show()
    spark1.sql("select * from global_temp.tblcustomer where prof='Pilot' limit 5").show()
    
    spark.catalog.listDatabases().show()
    spark1.catalog.listDatabases().show()

       
  
  }
}