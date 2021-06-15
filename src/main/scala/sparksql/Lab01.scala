package sparksql

import org.apache.spark.{SparkConf,SparkContext}

import org.apache.spark.sql.SQLContext

object Lab01 {
  
  def main(args:Array[String])=
  {
     val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab14-acc"))
    sc.setLogLevel("ERROR")
    //processmoviedatausingrdd(sc)
    
    val sqlc = new SQLContext(sc)
     
    val df = sqlc.read.format("csv")
    .option("InferSchema",true)
    .load("file:/home/hduser/movies.csv")    
    .toDF("movieid","moviename","releaseyear","rating","duration")
    
    val df1 = df.filter(df("rating") >= 3.5)
    
    //by default 20 records,truncate=true
    df1.show(100,false)
    
    //To get schema information
    df1.printSchema()
    
    
    
    
    
    
    
  }
  
  def processmoviedatausingrdd(sc:SparkContext)=
  {
    val rdd = sc.textFile("file:/home/hduser/movies.csv")
    
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.map(x => (x(0),x(1),x(2),x(3).toFloat,x(4)))
    
    val rdd3 = rdd2.filter(x => x._4 >= 3.5)
    
    rdd3.foreach(println)
  }
  
  
}