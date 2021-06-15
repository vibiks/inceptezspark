package sparkmaven

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf


object Lab02 {
   
  def main(args:Array[String]):Unit=
  {
       val sc = new  SparkContext(new SparkConf().setAppName("Lab02").setMaster("local"))
       
       val rdd = sc.textFile("file:/home/hduser/test.txt")
       
       rdd.foreach(println)
       
  }
}