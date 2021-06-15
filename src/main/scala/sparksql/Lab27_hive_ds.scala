package sparksql
import org.apache.spark.sql.SparkSession

case class custinfo(custid:Int,firstname:String,lastname:String,city:String,age:Int,createdt:String,transactamt:Int)
object Lab27_hive_ds {
  
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab23-SQL").master("local")
    .config("hive.metastore.uris","thrift://localhost:9083")
    .enableHiveSupport().getOrCreate()
    
    import spark.implicits._
    
    val df = spark.sql("select * from customer")
    
    val ds = spark.sql("select * from customer").as[custinfo]
    
    val ds1 = ds.filter(ds("city") === "chennai")
    
    val ds2 = ds.filter(x => x.city == "chennai")
  }
  
}