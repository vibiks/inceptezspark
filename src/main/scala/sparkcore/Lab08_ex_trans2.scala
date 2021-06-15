package sparkcore
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
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
object Lab08_ex_trans2
{
 def main(args:Array[String])=
  {
    val sc = new SparkContext(new SparkConf()
    //.setMaster("local")
    .setAppName("lab08"))
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hive/data/txns")
    
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.filter(x => x(7) == "Texas")
    
    rdd2.persist(MEMORY_AND_DISK)
    
    println("==============================")    
     val rdd3 = rdd2.map(x => x(3).toFloat)
    
    //select sum(salesamt) from txns
    println("Total Texas sales:" + rdd3.sum().round)
    
    println("==============================")
    
    
      //select product,1 as cnt from tnxs
     val rdd4 = rdd2.map(x => (x(5),1))
     
     //select product, sum(cnt) as totalsales from txns group by product
     val rdd5 = rdd4.reduceByKey((x,y) => x + y)
     
     //select product,totalsales from tnxs order by totalsales desc
     val rdd6 = rdd5.sortBy(x => x._2,false)
     
     //rdd3.foreach(println)    
     
     
     //select product,totalsales from tnxs limit 1
     val maxproduct = rdd6.first()
    
    println("=============Sales By Payment=================")
    
     //select paymenttype, cast(salesamt,float) from txns
    val rdd7 = rdd2.map(x => (x(8),x(3).toFloat))
    
    
    //select paymenttype,sum(salesamt) from txns group by paymenttype
    //val rdd8 = rdd7.reduceByKey((a,b) => a + b)
     val rdd8 = rdd7.reduceByKey(_ + _)
    
     rdd8.foreach(println)
    
    
  }
 
 
 
  
  
}