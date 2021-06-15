package sparkcore
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD

/*
---------------------------------
6. Data For Task
---------------------------------
Load txns file from /home/hduser/hive/data location into RDD

txnid,txndate,custid,salesamt,category,product,city,state,paymenttype

00000000,06-26-2011,4007024,040.33,Exercise & Fitness,Cardio Machine Accessories,Clarksville,Tennessee,credit

Accomplish the followings:-

select sum(salesamt) from txns where state='Texas'
1. Sum of sales for texas state

select count(txnid),product from txns where state = 'Texas' group by product order by count(txnid) by desc limit 1
2. Max number of product sales in texas

select paymenttype, sum(salesamt) from txns where state = 'Texas' group by paymenttype 
3. Sales by payment type in texas


 */
object Lab09_ex_trans_arg
{
 def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    //.setMaster("local")
    .setAppName("lab09"))
    sc.setLogLevel("ERROR")
    if(args.length > 0)
    {
    val filename = args(0)  
    val rdd = sc.textFile(filename)
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.filter(x => x(7) == "Texas")
    println("==============================")    
    gettotalsalesintexas(rdd2)
    
    println("==============================")
    getmaxproductintexas(rdd2)
    
    println("=============Sales By Payment=================")
    getsalesbypaymenttype(rdd2)
    }
    else
    {
      println("Filepath not passed as parameter")
    }
    
  }
 
 
  def gettotalsalesintexas(rdd:RDD[Array[String]])=
  {
    
    //select salesamt from txns
    val rdd1 = rdd.map(x => x(3).toFloat)
    
    //select sum(salesamt) from txns
    println("Total Texas sales:" + rdd1.sum().round)
    
  }
 
  def getmaxproductintexas(rdd:RDD[Array[String]])=
  {
      //select product,1 as cnt from tnxs
     val rdd1 = rdd.map(x => (x(5),1))
     
     //select product, sum(cnt) as totalsales from txns group by product
     val rdd2 = rdd1.reduceByKey((x,y) => x + y)
     
     //select product,totalsales from tnxs order by totalsales desc
     val rdd3 = rdd2.sortBy(x => x._2,false)
     
     //rdd3.foreach(println)
     
     //select product,totalsales from tnxs limit 1
     val maxproduct = rdd3.first()
     
     println(s"Max sales product : ${maxproduct._1} and sales count: ${maxproduct._2}")
     
     
  }
  
  def getsalesbypaymenttype(rdd:RDD[Array[String]])=
  {
    //select paymenttype, cast(salesamt,float) from txns
    val rdd1 = rdd.map(x => (x(8),x(3).toFloat))
    
    
    //select paymenttype,sum(salesamt) from txns group by paymenttype
    //val rdd2 = rdd1.reduceByKey((a,b) => a + b)
    val rdd2 = rdd1.reduceByKey(_ + _)
    
    rdd2.foreach(println)
    
        
  }
  
  
  
}