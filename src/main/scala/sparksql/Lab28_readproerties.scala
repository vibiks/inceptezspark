package sparksql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.io.FileInputStream

object Lab28_readproperties {
  
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab23-SQL").master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    
    //read data from properties file using th param --properties.file
    val filepath = spark.sparkContext.getConf.get("spark.inputpath")
    
    val df = spark.read.format("csv").load(filepath)
    
    df.show()
    
  }
    
}