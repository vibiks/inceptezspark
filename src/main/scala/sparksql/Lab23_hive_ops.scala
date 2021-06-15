package sparksql

import org.apache.spark.sql.SparkSession



object Lab23_hive_ops 
{
  def main(args:Array[String])=
  {
    
    val spark = SparkSession.builder().appName("Lab23-SQL")
    .config("spark.sql.warehouse.dir","file:/tmp/warehouse")
    .master("local").enableHiveSupport().getOrCreate()
    
    //spark.conf.set("spark.sql.warehouse.dir","file:/tmp/warehouse")    
    spark.sparkContext.setLogLevel("ERROR")
    
     val df = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","customer")
    .option("driver","com.mysql.cj.jdbc.Driver").load()
    
    //df.show()
    
    df.write.saveAsTable("tblcustomer")
    
    val df1 = spark.sql("select * from tblcustomer")
    
    //println("Data written")
    
    spark.sql("create table if not exists tblmarks(studid int,m1 int,m2 int,m3 int) row format delimited fields terminated by ','")
    
    spark.sql("load data local inpath '/home/hduser/marks.csv' into table tblmarks")
    
    println("Table created and loaded")
    
  }
  
}