package sparkcore
import org.apache.spark.{SparkConf,SparkContext}
/*
Given filename student.csv

student.csv
1,Lokesh
2,Bhupesh
3,Amit
4,Ratan
5,Dinesh

marks.csv
1~90~80~95
2~88~90~89
3~78~76~70
4~92~69~89
5~88~70~86


Accomplish the followings:-

1. Load the files into RDD and get the studentid,studentname,mark1,mark2,marks3,totalmarks
2. Get the highest and lowest totalmarks scored  
 */


object Lab12_join {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab12-joins"))
    sc.setLogLevel("ERROR")
    
    //1,Lokesh
    val srdd = sc.textFile("file:/home/hduser/students.csv")
    
    //1~90~80~95
    val mrdd = sc.textFile("file:/home/hduser/marks.csv")
    
    //Array("1","Lokesh")
    val srdd1 = srdd.map(x => x.split(","))
    
    //("1","Lokesh")
    val srdd2 = srdd1.map(x => (x(0),x(1)))
    
    //Array("1","90","80","95")
    val mrdd1 = mrdd.map(x => x.split("~"))
    
    //("1",("90","80","95"))
    val mrdd2 = mrdd1.map(x => (x(0),(x(1).toInt,x(2).toInt,x(3).toInt)))
    
    //("1",("Lokesh",("90","80","95")))
    val joinrdd = srdd2.join(mrdd2)
    
    joinrdd.foreach(println)
    
    val studentdata = joinrdd.map(x => (x._1,x._2._1,x._2._2._1,x._2._2._2,x._2._2._3))
    
    println("=========StudentInfo====================")
    
    studentdata.foreach(println)
    
    val studenttotal = studentdata.map(x => (x._1,x._2,x._3,x._4,x._5,x._3 + x._4 + x._5))
    
    println("=========StudentInfo with totalmarks====================")
    
    studenttotal.foreach(println)
    
    println("=========StudentInfo with Lowest Mark====================")
    
    
    val sorteddata = studenttotal.sortBy(x => x._6,true,1)
    
    val lowestmark = sorteddata.first
    
    println(lowestmark)
  
    println("=========StudentInfo with Highest Mark====================")
  
    val highestmark = studenttotal.sortBy(x => x._6,false,1).first
    
    
    
    println(highestmark)
    
    
    
    
    
    
    
    
    
    
    
  }
}