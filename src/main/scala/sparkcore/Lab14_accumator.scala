package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

object Lab14_accumator {
  
   def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("lab14-acc"))
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log",3)
    
    val infocnt = sc.longAccumulator("INFO-CNT")
    
    val warncnt = sc.longAccumulator("WARN-CNT")
    
    val errorcnt = sc.longAccumulator("ERROR-CNT")
    
    rdd.foreach(x =>
      {
        if(x.contains("INFO"))
          infocnt.add(1)
        else if(x.contains("WARN"))
          warncnt.add(1)
        else if(x.contains("ERROR"))
          errorcnt.add(1)       
      })
     
    println("Info count:" + infocnt.value)
    println("Warn count:" + warncnt.value)
    println("Error count:" + errorcnt.value)
    
    
    
  }
  
}