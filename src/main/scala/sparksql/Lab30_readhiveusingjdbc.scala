package sparksql
import org.apache.spark.sql.SparkSession
object Lab30_readhiveusingjdbc 
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab30-SQL").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
     val df1 = spark.read.format("jdbc")
    .option("url","jdbc:hive2://localhost:10000/default")
    .option("user","hduser")
    .option("password","hduser")
    .option("dbtable","tblmarks")
    .option("driver","org.apache.hive.jdbc.HiveDriver")
    .load()    
    
    df1.show()
  }
  
}