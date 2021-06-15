package sparksql

import org.apache.spark.sql.{SparkSession,Row}

import org.apache.spark.sql.types._


object Lab06_read_df 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab06-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val custschema = StructType(List(
          StructField("Custid",IntegerType,true), 
          StructField("FName",StringType,true), 
          StructField("LName",StringType,true), 
          StructField("Age",IntegerType,true), 
          StructField("Prof",StringType,true)))

    /*
     There are 3 typical read modes and the default read mode is permissive.
     
			permissive — All fields are set to null and corrupted records are placed in a string column called _corrupt_record
			dropMalformed — Drops all rows containing corrupt records.
			failFast — Fails when corrupt records are encountered.
     */
    
    val df = spark
            .read
            .format("csv")
            .option("mode","PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            //.option("header",true)
            //.option("inferSchema",true)
            .schema(custschema)
            .load("file:/home/hduser/hive/data/custs")
            //.toDF("Custid","FName","LName","Age","Prof")
    
    
    //select * from customer limit 10        
    
    //select top 10 * from customer
            
            
    //val df1 = df.limit(10)
    
    df.limit(10).show()
    
    df.show()
    
    df.printSchema()

    println(df.schema)
    
    
    
  }
}