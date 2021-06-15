package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{current_timestamp,current_date}


object Lab25_hive_read
{
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab23-SQL").master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .enableHiveSupport().getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
        
    //From hive
    val student = spark.sql("select * from tblstudent")
   
    //From file    
    val mark = spark.read.format("csv").option("inferSchema",true).option("delimiter","~")
    .load("file:/home/hduser/marks.csv").toDF("studid","mark1","mark2","mark3")
     
    //select * from student inner join mark on student.studid = mark.studid
    //val studentmark = student.join(mark,Seq("studid"),"inner")
    
    mark.createOrReplaceTempView("tmpmark")
    
    student.createOrReplaceTempView("tmpstudent")
    
    val studentmark = spark.sql("select s.studid,s.studname,m.mark1,m.mark2,m.mark3 from tmpstudent s inner join tmpmark m on s.studid = m.studid")
    
    //add current timestamp
    
   val studentmark1 = studentmark.withColumn("load_ts", current_timestamp()).withColumn("load_dt", current_date())
       
    //write into mysql
    studentmark1.write.format("jdbc").option("url","jdbc:mysql://localhost/custdb")
    .mode("append")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","tblstudmark")
    .option("driver","com.mysql.cj.jdbc.Driver").save()
    
    println("Written into mysql")
    
    //write into hive - by default parquet
    studentmark1.write.mode("append").format("orc").saveAsTable("retail.tblstudmark")
    
    //partition
    //studentmark1.write.mode("append").format("orc").partitionBy("studname").saveAsTable("retail.tblstudmark")
    
    
    
    println("Written into hive")
    
    //Read from file -> Read from hive -> join -> write into mysql
    
    
  
  }
  
}