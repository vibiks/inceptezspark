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

select s.studid,s.studname,m.m1,m.m2,m.m3,(m.m1 + m.m2 + m.m3)as totalmarks 
from student s join marks m on s.studid = m.studid
1. Load the files into RDD and get the studentid,studentname,mark1,mark2,marks3,totalmarks


select s.studid,s.studname,m.m1,m.m2,m.m3,(m.m1 + m.m2 + m.m3) as totalmarks 
from student s join marks m on s.studid = m.studid order by totalmarks asc limit 1

select s.studid,s.studname,m.m1,m.m2,m.m3,(m.m1 + m.m2 + m.m3) as totalmarks 
from student s join marks m on s.studid = m.studid order by totalmarks desc limit 1

2. Get the highest and lowest totalmarks scored  
 */


object Lab13_collect {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab13-joins"))
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
    //val joinrdd = srdd2.leftOuterJoin(mrdd2)
    //val joinrdd = srdd2.rightOuterJoin(mrdd2)
    //val joinrdd = srdd2.fullOuterJoin(mrdd2)
    
    joinrdd.foreach(println)
    
    val studentdata = joinrdd.map(x => (x._1,x._2._1,x._2._2._1,x._2._2._2,x._2._2._3))
    
    println("=========StudentInfo====================")
    
    studentdata.foreach(println)
    
    val studenttotal = studentdata.map(x => (x._1,x._2,x._3,x._4,x._5,x._3 + x._4 + x._5))
    
    println("=========StudentInfo with totalmarks====================")
    
    studenttotal.foreach(println)
    
    println("=========StudentInfo with Lowest Mark====================")
    
    val studentarr = studenttotal.collect()
    
    val studlst = studentarr.toList
    
    val studsort = studlst.sortBy(x => x._6)
    
    println("=========StudentInfo with Lowest Mark====================")
    
    println(studsort(0))
    
    println("=========StudentInfo with Highest Mark====================")
     
    println(studsort.last)
    //or
    //println(studsort(studsort.length-1))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}