package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.udf

import org.apache.spark.sql.Dataset


object Lab20_udf {
  
  case class customer(custid:Int,fname:String,lname:String,age:Int,prof:String)
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab19-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","prof")
    
    def getdiscount(age:Int)=
    {
      if(age < 10)
        5
      else if(age < 20)
        10
      else if(age  < 40)
        15
      else if(age < 60)
        20
      else
        25
    }
    
    val getdiscountper = udf(getdiscount _)
    
    val df1 = df.withColumn("discountpercent", getdiscountper(col("age")))
    
    df1.show()
    
    df.select(getdiscountper(col("age"))).show()
    
    //when we want to use in sql query, register the udf method as below
    spark.udf.register("getdiscountper", getdiscount _)
    
    df.createOrReplaceTempView("customer")
    
    spark.sql("select custid,fname,lname,age,prof,getdiscountper(age) as discountper from customer").show()
    
    val fnprime = udf(isprime)
    
    df.select(col("age"),fnprime(col("age"))).show()
    
    //spark.sql("select p,n,r,calcualtepnr(p,n,r) as interest from account")
    
  }
  
  val isprime = (a:Int) =>
    {
      var isprime = true
      for(i <- 2 to  a-1)
      {
        if( a % i == 0)
          isprime = false
      }
      isprime
    }
}