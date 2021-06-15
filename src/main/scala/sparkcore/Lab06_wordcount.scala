package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

object Lab06_wordcount {
  
  def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    .setMaster("local[1]")
    .setAppName("lab06-Wordcount"))
    sc.setLogLevel("ERROR")
    
    //Stage-1
    val rdd = sc.parallelize(List("spark,scala,hive","hive,java,spark","spark,python,scala"),2)
    val rdd1 = rdd.flatMap(x => x.split(","))
    val rdd2 = rdd1.map(x => (x,1))
    
    //Stage-2
    val rdd3 = rdd2.reduceByKey((a,b) => a + b,1)
    rdd3.foreach(println)
    
  }
}