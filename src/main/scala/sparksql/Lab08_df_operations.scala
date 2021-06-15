package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab08_df_operations 
{
   def main(args:Array[String])
  {
    val spark = SparkSession.builder().appName("Lab08-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //select state,city,sum(amount),max(amount),min(amount) from txns group by state,city
    
    val dftxn = spark.read.format("csv")
            .option("delimiter","|")
            .option("inferSchema",true)
            .load("file:/home/hduser/hive/data/txns")
            .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
    
   /* val dfagg = dftxn.groupBy("state","city")
                .agg(sum("amount"),max("amount"),min("amount"))
    
//select state,city,round(sum(amount),2) as totalsales,max(amount) maxsales,min(amount) minsales from txns group by state,city
     val dfagg1 = dftxn.groupBy("state","city")
                .agg(round(sum("amount"),2).alias("totalsales"),
                 max("amount").alias("Maxsales"),
                 min("amount").alias("MinSales"))*/
                
    //select round(sum(amount),2) as totalsales,max(amount) maxsales,min(amount) minsales from txns
     /*val dfagg2 = dftxn.agg(round(sum("amount"),2).alias("totalsales"),
                 max("amount").alias("Maxsales"),
                 min("amount").alias("MinSales"))*/             
    
                 
    //dfagg2.show()
 
    //No of records
    //dftxn.count()
    
    //val rec = dftxn.first()
    
    //get 10 records
    //val recs = dftxn.head(10)
    
    //select txnid,txndate,amount,city,state from txn where state='California' order by amount,txndate
    
    val df1 = dftxn.select(col("txnid"),col("txndate"),col("amount"),col("city"),col("state"))
                   .where(col("state") === "California")
                   .orderBy("amount","txndate")

      //select txnid,txndate,amount,city,state from txn where state='California' order by amount desc               
    val df2 = dftxn.select(col("txnid"),col("txndate"),col("amount"),col("city"),col("state"))
                   .where(col("state") === "California")
                   .orderBy(col("amount").desc)               
    
                   
   //use sort
   //select txnid,txndate,amount,city,state from txn where state='California' order by amount,txndate
    
    val df3 = dftxn.select(col("txnid"),col("txndate"),col("amount"),col("city"),col("state"))
                   .where(col("state") === "California")
                   .sort("amount","txndate")

      //select txnid,txndate,amount,city,state from txn where state='California' order by amount desc               
    val df4 = dftxn.select(col("txnid"),col("txndate"),col("amount"),col("city"),col("state"))
                   .where(col("state") === "California")
                   .sort(col("amount").desc()) 
                   
   df4.show()
  
    
    
    
    
    
    
    
    
  }
   
   
  
}