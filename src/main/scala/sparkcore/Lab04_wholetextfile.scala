package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

object Lab04_wholetextfile extends App 
{
   val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("lab03-readfile"))
    
    sc.setLogLevel("ERROR")
    
    val rdd = sc.wholeTextFiles("file:/home/hduser/transcust,hdfs://localhost:54310/user/hduser/transcust")
    rdd.foreach(println)
    
    print("============Get only the filenames=================")
    val rdd1 = rdd.map(x => x._1)
    rdd1.foreach(println)
    
    
    print("============Get only the content=================")
    val rdd2 = rdd.map(x => x._2)
    rdd2.foreach(println)
    
    
    
    
    
  
}