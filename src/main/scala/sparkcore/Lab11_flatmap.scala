package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

object Lab11_flatmap {
  
  def main(args:Array[String])=
  {
    val lst = List(List("Hadoop","Hive","Java"),List("Spark","Scala","Python"))
    
    //val lst1 = List("Hadoop","Hive","Java","Spark","Scala","Python")
    
    val lst2 = lst.flatMap(x => x)
    
    println(lst)
    println(lst2)
    
    val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab11"))
    sc.setLogLevel("ERROR")
    
    val lst3 = List("Hadoop-Spark","Scala-Java","Hadoop-Java","Spark-Scala")
    
    val rdd = sc.parallelize(lst3)
    
    //rdd1(Array("Hadoop","Spark"),Array("Scala","Java"),Array("Spark","Scala"))
    
    val rdd1 = rdd.map(x => x.split("-"))
    
    //rdd2("Hadoop","Spark","Scala","Java","Spark","Scala")
    
    val rdd2 = rdd1.flatMap(x => x)
    
    //rdd("Hadoop-Spark","Scala-Java","Hadoop-Java","Spark-Scala")
     //rdd3("Hadoop","Spark","Scala","Java","Spark","Scala")
    val rdd3 = rdd.flatMap(x => x.split("-"))
    
    
    
    
    
    
    
    
    
    
    
    

  }
  
}