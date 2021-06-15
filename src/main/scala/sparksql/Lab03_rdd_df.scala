package sparksql

import org.apache.spark.sql.SparkSession

//spark-submit --class sparksql.Lab03_rdd_df --master local sparkworkouts.jar
object Lab03_rdd_df {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab03").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.filter(x => x.length == 5)
    
    val rdd3 = rdd2.map(x => (x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
    //rdd3.foreach(println)
    
    import spark.implicits._
    
    val df = rdd3.toDF()
    
    df.show()
    
    df.printSchema()
    
    
    
  }
  
  
}