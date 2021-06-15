package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

/*
 Given a file name user.csv

id,topic,hits
Rahul,scala,120
Nikita,spark,80
Mithun,spark,1
myself,cca175,180

Accomplish the followings:-

Write spark code in scala which will remove the header part and create RDD of values as below,for all rows.
And also if id is "myself" than filter out row.

 */
object Lab07_header 
{
 def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab07-header"))
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/user.csv")
    
    val header = rdd.first()
    
    val rdd1 = rdd.filter(x => x != header)
    
    rdd1.foreach(println)
    
    println("===========================")
    
    val rdd2 = rdd1.filter(x => !x.contains("myself"))
    rdd2.foreach(println)
    
    
    
    
    
    
    
    
  }
  
  
  
}