package sparksql

import org.apache.spark.sql.{SparkSession,Row}

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.col


object Lab07_df_api {
  
   def main(args:Array[String])
  {
    val spark = SparkSession.builder().appName("Lab07-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    /*create table txn(txnid int null,txndate string null,custid string null,amount float,null,
    category string null,product string null,city string null,state string null,paymenttype string null) 
    
    */
    
    val txnschema = StructType(List(StructField("txnid", IntegerType, true),
        StructField("txndate", StringType, true),
        StructField("custid", StringType, true),
        StructField("amount", FloatType, true),
        StructField("category", StringType, true),
        StructField("product", StringType, true),
        StructField("city", StringType, true),
        StructField("state", StringType, true),
        StructField("paymenttype", StringType, true)))
    
     val df = spark.read.format("csv")
    .schema(txnschema)
    .option("delimiter","|")
    .load("file:/home/hduser/hive/data/txns")
    
    //select txnid,amount,prouct,city,state from txn where state = 'Texas' or state = 'California'
    
    //col type import from org.apache.spark.sql.functions.col
    val df1 = df.select(col("txnid"),col("amount"),col("product"),col("city"),col("state"))
    
    //or
    import spark.implicits._
    
    val df11 = df.select($"txnid",col("amount"),'product,df("city"),col("state"))
    
    
    val df2 = df11.filter(col("state") === "Texas")
    //or
    val df3 = df1.filter(df1("state") === "Texas")
    //or
    
    val df4 = df1.filter($"state" === "Texas")
    
    //or
    val df5 = df1.filter('state === "Texas")
    
    val df41 = df1.filter($"state" === "Texas" || col("state") ==="California")
    
    val df412 = df2.where("state = 'Texas'")
    
    val df413 = df2.where($"state" === "Texas" || col("state") ==="California")
    
       
    df412.show()
    
    //select distinct city, state from txn
    df.select(col("city"),col("state")).distinct().show()
    
    //remove duplicates
    
    val df6 = df.distinct()
     
     
    //select distinct city as uscity, state as usstate from txn
    df.select(col("city").alias("uscity"),col("state").alias("usstate")).distinct().show()
    
    
    
    /*df2.show()
    //or
    df.select(col("txnid"),col("amount"),col("product"),col("city"),col("state"))
    .filter(col("state") === "Texas")
    .show()*/
    
    
    
    
    
    
    
    
        
  }
}