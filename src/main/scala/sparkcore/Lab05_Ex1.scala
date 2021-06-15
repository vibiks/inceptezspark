package sparkcore
import org.apache.spark.{SparkConf,SparkContext}
/*
 ---------------------------------
1. Data For Task
---------------------------------
Inceptez1.txt
I am learning Apache Spark from Inceptez Learning Resources
I am learning Apache Hadoop from Inceptez Learning Resources
I have created my technical profile at www.QuickTechie.com
I am learning Apache Spark from Training4exam Learning Resources

Accomplish the followings:-

1. Create this text file in HDFS
2. Once file is created, write the spark application which will read from HDFS as an RDD
3. Once RDD loaded, do the line count of this RDD
  
 */

object Lab05_Ex1 
{
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("lab05-Ex01"))
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("hdfs://localhost:54310/user/hduser/Inceptez1.txt")
    println(s"Total lines in the file:${rdd.count()}")
    
  }
  
}