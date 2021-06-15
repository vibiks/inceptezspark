package sparkmaven


import org.apache.spark.sql.SparkSession


object Lab03 {
   
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab01").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.format("csv").load("file:/home/hduser/hive/data/custs")
    df.show()
  }
}