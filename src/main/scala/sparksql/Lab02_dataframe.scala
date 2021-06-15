package sparksql

import org.apache.spark.{SparkConf,SparkContext}

import org.apache.spark.sql.SQLContext

object Lab02_dataframe {
  
  def main(args:Array[String])=
  {
     val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab02-sql"))
    sc.setLogLevel("ERROR")
    
    val sqlc = new SQLContext(sc)
     
    val df = sqlc.read.format("csv")
    .option("inferSchema",true) //by default - false
    .option("header",true) //by default - false
    .load("file:/home/hduser/movies.csv")
    
    val df1 = df.filter(df("rating") >= 3.5)
    
    //by default 20 records,truncate=true
    df1.show(100,false)
    
    //To get schema information
    df1.printSchema()    
    
    //To save the data
    df1.write.format("csv").save("file:/home/hduser/moviesrating")
    
    println("Data written successfully")
    
    
    
  }
  
  
}