package sparkcore
import org.apache.spark.{SparkConf,SparkContext}


object Lab10_coursefeecalc {
 
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab10"))
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/coursefee.txt",2)
    
    val rdd1 = rdd.map(x => x.split(",")).coalesce(1)
    
    val rdd2 = rdd1.map(x => (x(0),x(1).toInt,x(2).toInt)).repartition(2)
    
    val rdd3 = rdd2.coalesce(1)
    
    val rdd4 = rdd3.map(x => List(x._1,(x._2 * x._3) /  100,(x._2 * x._3) /  100 + x._2, x._2,x._3))
    
    //rdd3.foreach(println)
    
    val rdd5 = rdd4.map(x => x.mkString(","))
    
    val headerstr = List("CourseName,Taxamount,Totalfee,actualfee,TaxPercent")
    
    val rddheader = sc.parallelize(headerstr)
    
    val rdd6 = rddheader.union(rdd5)
    
    rdd6.coalesce(1).saveAsTextFile("file:/home/hduser/coursefeedetails")
    
    println("Data stored in file")
    
    
  
  }
  
  

}