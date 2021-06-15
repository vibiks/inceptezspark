package sparksql

import org.apache.spark.sql.SparkSession


object Lab31_readhiveschema 
{
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab23-SQL").master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .enableHiveSupport().getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
     spark.sql("show databases").show()
    
    val df = spark.sql("show tables from retail")
    
    val tblist = df.collect()
    
    
      tblist.foreach(x=>
      {
        spark.sql("use " + x.getAs(0).toString())
        val dfcol = spark.sql("describe " + x.getAs(1).toString())
        dfcol.show()
      })
    
    
  
  }
  
}