package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._


object Lab09_df_joins 
{
  def main(args:Array[String])
  {
    val spark = SparkSession.builder().appName("Lab09-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //select * from student
    val student = spark.read.format("csv").option("inferSchema",true).load("file:/home/hduser/students.csv").toDF("studid","studname")
    
    //select * from mark
    val mark = spark.read.format("csv").option("inferSchema",true).option("delimiter","~").load("file:/home/hduser/marks.csv").toDF("studid","mark1","mark2","mark3")
    
    //select * from student inner join mark on student.studid = mark.studid
    val studentmark = student.join(mark,student("studid") === mark("studid"),"inner")
    //or
    val studentmark1 = student.join(mark,"studid")
    
    val leftstudentmark = student.join(mark,student("studid") === mark("studid"),"left_outer")
    
    val rightstudentmark = student.join(mark,student("studid") === mark("studid"),"right_outer")
    
    val fullstudentmark = student.join(mark,student("studid") === mark("studid"),"full_outer")
    
    studentmark.show()
    
    
    
  }
}