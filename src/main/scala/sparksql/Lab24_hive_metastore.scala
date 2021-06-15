package sparksql

import org.apache.spark.sql.SparkSession


object Lab24_hive_metastore 
{
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab23-SQL").master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .enableHiveSupport().getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
     spark.sql("show databases").show()
    
    spark.sql("show tables from retail")
    
    
    spark.sql("drop table if exists tblmarks")
    
    spark.sql("create table tblmarks(studid int,m1 int,m2 int,m3 int) row format delimited fields terminated by '~'")
    
    spark.sql("load data local inpath '/home/hduser/marks.csv' into table tblmarks")
    
    println("Table created and loaded in hive")
    
    
  
  }
  
}