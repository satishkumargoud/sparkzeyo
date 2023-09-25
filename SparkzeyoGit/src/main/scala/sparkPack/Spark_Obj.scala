package sparkPack
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
object Spark_Obj {
  
def main(args: Array[String]): Unit = {
      
 val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
 val sc=new SparkContext(conf)
 sc.setLogLevel("Error")
 val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
 import spark.implicits._
 
 val hc=new HiveContext(sc)
 import hc.implicits._
 
 val fgh=spark.read.format("json").option("multiLine","true")
					.load("file:///D:/variables/data/complexdata/Array3.json")
					fgh.show(false)
					fgh.printSchema()
  }
  
}