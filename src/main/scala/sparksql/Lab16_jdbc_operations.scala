package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object Lab16_jdbc_operations {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab16-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","customer")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()
    
    df.filter("city='chennai'").show()
    
    val df1 = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","(select * from customer where city='chennai') as t")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()    
    
    df1.show()
    
    //When reading JDBC data sources, 
    //users need to specify all or none for the following options: 
    //'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'
    
    
    //Partition column type should be numeric, date, or timestamp
    
     val df2 = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","tblcustomer")
    .option("driver","com.mysql.cj.jdbc.Driver") 
    
    .option("numPartitions",5)
    .option("partitionColumn", "custid")
    .option("lowerBound", 0)
    .option("upperBound", 5000000)
    .load()
    
    val lst = List(("tblcustomer","cutid",5,0,100),("customer","cutid",5,0,100),("customer_bkp","cutid",5,0,100))
    
    lst.foreach(x =>
      {
     
    val df2 = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable",x._1)
    .option("driver","com.mysql.cj.jdbc.Driver")    
    .option("numPartitions",x._3)
    .option("partitionColumn", x._2)
    .option("lowerBound", x._4)
    .option("upperBound", x._5)
    .load()
    
    
    df2.write.format("csv").save("file:/home/hduser/" + x._1)
    
    })
    /*
    100 / 5 = 20
    select * from customer where custid > 0 and custid <=20
    select * from customer where custid > 20 and custid <=40
    select * from customer where custid > 40 and custid <=60
    select * from customer where custid > 60 and custid <=80
    select * from customer where custid > 80 and custid <=100
    
    */
    
      
    
    
    
   
    
   
    
    
    
    
    
    //df.show()
    
  }
}