package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Lab01 {
  
  def main(args:Array[String])=
  {
    println("Welcome to spark...")
    
    /*Step -1
     Create sc object from SparkContext class
     Sparkcontext takes parameters
     			1. Application Name
     			2. master
     			
     we can conf object from SparkConf class and set the 2 parameters(appname,master)
     and pass into SparkContext
     
     */
    
    val conf = new SparkConf()
    
    //conf.setMaster("local")
    conf.setAppName("First-Spark")
    
    //val sc = new SparkContext(new SparkConf().setAppName("First-Spark").setMaster("local"))
    
    val sc = new SparkContext(conf)
    val lst = List(10,20,30)
    
    /* Step -2:
     Create RDD by using SparkContext
    */
    val rdd = sc.parallelize(lst)
    
    val total = rdd.sum()
    
    print(s"Total:$total")
    
    
    
    
    
    
    
  }
  
}