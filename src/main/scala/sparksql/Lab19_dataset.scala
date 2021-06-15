package sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._



object Lab19_dataset {
  
  /*
 
 Dataset: It is the combination of RDD and dataframe.
 
 A Dataset is a strongly-typed, immutable collection of objects that are mapped to a relational schema. 
 
 Introduced in spark version 1.6
 
 
 Dataset brings compile time safety, the object oriented programming style of RDD, 
 and the advances of dataframes together.

 Datasets also introduced the concept of encoders.
 
 Encoders work as translators among JVM objects and Spark internal binary format. 
 
 The tabular representation of data with schema is stored in Spark binary format. 
 
 Encoders allow operations on serialized data. 
 
 Encoders allow the access of individual attributes without the need to de-serilize an entire object. 
 
 Thus, it reduces serialization efforts and load.
 
 Agg and sorting can be done over serialized data
 
* */
  case class customer(custid:Int,fname:String,lname:String,age:Int,prof:String)
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab19-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
     
    import spark.implicits._
    
    val df = spark.read.format("csv").option("inferschema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","prof")
    
    val ds = df.as[customer]
    
    
    val customerfilter = (cust:customer)=>
    {
      if(cust.prof == "Pilot")
        true 
      else
        false
    }
    
    ds.filter("prof = 'Pilot'").show()
    
    
    ds.filter(x => x.prof == "Pilot").show()
    
    ds.filter(customerfilter).show()
    
    //convert dataset to dataframe
    val df1 = ds.toDF()
    
    //convert dataframe to rdd
    val rddf = df.rdd
    
    val firstrow = rddf.first()
    
    print(firstrow.getInt(0))
    
    
    //convert dataset to rdd
    val rdds = ds.rdd
    
    val frow = rdds.first()
    
    print(frow.custid)
    
    
    val custlist = Seq(customer(101,"a","b",23,"Teacher"),customer(102,"karthik","Magesh",32,"Lawyer"))
    
    val custds1 = spark.createDataset(custlist)
    
    val custrdd = spark.sparkContext.parallelize(custlist)
    
    
    
    val custdf = custrdd.toDF()
    
    //rdd to dataset
    val custds = spark.createDataset(custrdd)
    
    custds.show()
    
    val rddlst = spark.sparkContext.parallelize(List(10,20,30,40))
    
    val ds1 = spark.createDataset(rddlst)
    
    ds1.show()
    
    
    
  }
}