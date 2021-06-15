package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab12_sqlquery_join {
  
   def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab12-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
        
    //df.filter(x => x.getAs(4).toString == "Pilot")
    
     val transdf = spark.read.format("csv")
    .option("inferSchema",true)   
    .option("delimiter","|")
    .load("file:/home/hduser/hive/data/txns")
    .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype").limit(100)
    
     val custdf = spark.read.format("csv")
    .option("inferSchema",true)    
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","profession")
    
    transdf.createOrReplaceTempView("txns")
    
    custdf.createTempView("custs")
    
    val df1 = spark.sql("""select t.txnid,c.custid,c.age,c.profession,t.city,t.state from txns t 
                           inner join custs c on t.custid=c.custid""")
    
    
    df1.createOrReplaceTempView("custs")
    
    spark.sql("select city,state from custs").show() 
                           
    
    
  }
  
  
}