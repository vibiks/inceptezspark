package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

object Lab03_readfiles {
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("lab03-readfile"))
    
    sc.setLogLevel("ERROR")
    
    println("=========Read one file==============")
    val rdd = sc.textFile("file:/home/hduser/test.txt")
    rdd.foreach(println)
    
    
    println("=========Read multiple files==============")
    val rdd1 = sc.textFile("file:/home/hduser/file1,file:/home/hduser/file2,file:/home/hduser/file3")
    rdd1.foreach(println)
    
    println("=========Read all files from folder==============")
    val rdd2 = sc.textFile("file:/home/hduser/transcust")
    rdd2.foreach(println)
    
    
    println("=========Read only textfile from folder==============")
    val rdd3 = sc.textFile("file:/home/hduser/transcust/f*")
    rdd3.foreach(println)
    
    println("=========Read from multiple folders==============")
    val rdd4 = sc.textFile("file:/home/hduser/transcust,file:/home/hduser/transcust1")
    rdd4.foreach(println)
    
    
    println("=========Read from hdfs file==============")
    val rdd5 = sc.textFile("hdfs://localhost:54310/user/hduser/test.txt")
    rdd5.foreach(println)
    
    
    
    println("=========Read from local and hdfs file==============")
    val rdd6 = sc.textFile("file:/home/hduser/test.txt,hdfs://localhost:54310/user/hduser/test.txt")
    rdd6.foreach(println)
    
    
    
  }
  
}