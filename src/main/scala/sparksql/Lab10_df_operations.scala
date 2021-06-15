package sparksql
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

object Lab10_df_operations 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab10-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv").option("inferschema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","prof")
    
    //Add column to the dataframe
    
    val df1 = df.withColumn("country",lit("USA"))
    
    val df2 = df1.withColumn("fullname",concat(col("fname"),lit(" "),col("lname")))
    
    val df3 = df2.withColumn("is_even", col("age") % 2 === 0)
    
    val df4 = df3.withColumn("category", when(col("age") <  20,"Child")
              .when(col("age") <  40,"Young")
              .when(col("age") < 60 ,"Old")
              .otherwise("Very Old"))
              
    val df5 = df4.withColumn("Id", monotonically_increasing_id())
    
    //Rename column    
    val df6 = df5.withColumnRenamed("Id", "SeqId").withColumnRenamed("category", "agecategory")
    
    //Drop column from dataframe
    val df7 = df6.drop("Seqid")
    
    val df8 = df7.drop("agecategory","fullname","is_even")
    
    //Deal with null
    df.filter("prof is not null").show()
    df.where("prof is null").show()
    //or
    df.filter(col("prof").isNotNull).show()
    df.filter(col("prof").isNull).show()
    
    //Replace all integer and long columns
    df.na.fill(0).show(false)
    
    //Replace all string columns
    df.na.fill("Unknown").show(false)
    
    //Replace for one specific string columns
    df.na.fill("Unknown",Array("prof")).show(false)
    
    df.na.fill(0,Array("age")).show(false)
    
    //Replace for specific string columns
    df.na.fill("NA",Array("fname","lname","prof")).show(false)
    
    //Delete row which contains any column value is  null
    df.na.drop().show()
    
    //Delete row where specific column is null
    df.na.drop(Array("prof")).show()
    
    
    df.select(col("age").cast(StringType),col("custid").cast(StringType))
    
    df.withColumn("custid1",col("custid").cast(LongType)).show()
    
    //Get summary info(count,max,min,stddev,mean)
    df.describe("prof").show()       
    
    
    
    df1.printSchema()
   
    
    
  }
}