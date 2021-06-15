package sparkmaven


import org.apache.spark.sql.SparkSession


object Lab04 {
   
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab01").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.format("csv").load("file:/home/hduser/hive/data/custs")
    
     //Table should already exists
    df.write.format("org.apache.phoenix.spark")
    .option("table", "CUSTOMER1").option("zkUrl", "localhost:2181").save()
    
    df.show()
  }
}