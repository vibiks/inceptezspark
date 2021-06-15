package sparkcore
import org.apache.spark.{SparkConf,SparkContext}

object Lab02_factorial {
  
  def main(args:Array[String])=
  {
    //write spark program to find the factorial of each number in the list
    val lst = List(5,3,7)
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("lab02"))
    val rddlst = sc.parallelize(lst)
    val factrdd = rddlst.map(findfactorial)
    factrdd.foreach(println)    
    
  }
  
  def findfactorial(a:Int):Int=
  {
    var fact = 1
        for(i <- 2 to a)
        {
          fact = fact * i
        }
    return fact  
  }
}

//5! = 1 x 2 x 3 x 4 x 5