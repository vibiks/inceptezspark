package sparksql

import org.apache.spark.sql.SparkSession

object Lab04_list_rdd_df {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab03").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
     val rdd = sc.parallelize(List(("Raja",24),("Kumar",30),("Naveen",29),("Praveen",31)))
    
   
    //convert rdd to df, use implicits
    import spark.implicits._
    
    val df = rdd.toDF("UserName","UserAge")
    
    df.show()
    
    df.printSchema()
    
    
  }
  
  
}