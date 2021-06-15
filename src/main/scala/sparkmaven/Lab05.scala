package sparkmaven


import org.apache.spark.sql.SparkSession


object Lab05 {
   
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab01").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("org.apache.phoenix.spark")
    .option("table", "CUSTOMER").option("zkUrl", "localhost:2181").load()
    
    df.show()
  }
}