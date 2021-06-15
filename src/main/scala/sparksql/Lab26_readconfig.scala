package sparksql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.io.FileInputStream

object Lab26_readconfig {
  
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab23-SQL").master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //SparkFiles.get -> get files passed from --files
    val file = org.apache.spark.SparkFiles.get("config.properties")
    
    val confdata = new FileInputStream(file)
    
    val prop = new Properties()
    
    prop.load(confdata)
    
    println(prop.getProperty("conurl"))
    println(prop.getProperty("username"))
    
     val df = spark.read.format("jdbc")
    .option("url",prop.getProperty("conurl"))
    .option("user",prop.getProperty("username"))
    .option("password",prop.getProperty("password"))
    .option("dbtable",prop.getProperty("table"))
    .option("driver",prop.getProperty("driver")).load()
    
    df.show()
    
    
  }
    
}