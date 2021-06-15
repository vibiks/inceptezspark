package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab22_datetime_ops {

  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab22-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv")
    .option("inferSchema",true).option("delimiter","|")    
    .load("file:/home/hduser/hive/data/txns")
    .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype").limit(20)
    
    df.show()
    
    df.printSchema()
    
    //Default date format - yyyy-MM-dd
    
    //Add current date
    val df1 = df.withColumn("load_dt", current_date())

    
    //Add current timestamp
    val df2 = df1.withColumn("load_ts", current_timestamp())

    df2.show()
    
    //unix_timestamp
    val df3 = df2.withColumn("load_uts", unix_timestamp())
    df3.show()
    
    //to_date -> convert string to a date
    val df4 = df3.withColumn("txndatef",to_date(col("txndate"),"MM-dd-yyyy"))
    
    df4.show()
    
    
    //date_format -> convert date from one format to another format and returns a string
    val df5 = df4.withColumn("txndatef1",date_format(col("txndatef"),"dd-MM-yyyy"))
    
    df5.show()
    
    //datediff and months_between
   df5.select( col("txndate"), current_date(), 
       datediff(current_date(),col("txndatef")).as("datediffindays"), 
       months_between(current_date(),col("txndatef")).as("months_between")
   ).printSchema()
   
   
   //Extract day,month,year
   df5.select( col("txndate"), 
          month(col("txndatef")).as("Month"), 
          year(col("txndatef")).as("Year"), 
          dayofmonth(col("txndatef")).as("day") 
     ).show()

   
   //add_months, date_add, date_sub
   df5.select(col("txndatef"), 
      add_months(col("txndatef"),3).as("add_months"), 
      add_months(col("txndatef"),-3).as("sub_months"), 
      date_add(col("txndatef"),4).as("date_add"), 
      date_sub(col("txndatef"),4).as("date_sub")).show()
 
     //year, month, month, dayofweek, dayofmonth, dayofyear, next_day, weekofyear 
     df5.select( col("txndatef"), year(col("txndatef")).as("year"), 
       month(col("txndatef")).as("month"), 
       dayofweek(col("txndatef")).as("dayofweek"), 
       dayofmonth(col("txndatef")).as("dayofmonth"), 
       dayofyear(col("txndatef")).as("dayofyear"), 
       next_day(col("txndatef"),"Sunday").as("next_day"), 
       weekofyear(col("txndatef")).as("weekofyear") 
   ).show()
   
   //to_timestamp() 
   df5.select(to_timestamp(col("txndate"),"MM-dd-yyyy")).show()
   
   //unix_timestamp()
   df5.select(unix_timestamp(col("txndate"),"MM-dd-yyyy").as("txndate_unixts")).show()

   
   val df6 = df5.withColumn("txndate_unix_ts",unix_timestamp(col("txndate"),"MM-dd-yyyy"))
   
   
   //dd-MM-yyyy HH:mm:ss
   

   df6.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts")).as("txndate-1")).show()
   
   df6.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd-MM-yyyy HH:mm:ss").as("txndate-1")).show()
   
   
   df6.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd-MMM-yyyy HH:mm:ss").as("txndate-1")).show()
   
   
   df6.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd-MMM-yyyy").as("txndate-1")).show()
   
   
   df6.select(col("txndate_unix_ts"),from_unixtime(col("txndate_unix_ts"),"dd/MM/yyyy HH:mm:ss").as("txndate-1")).show()
   
   
    
  }
  
}