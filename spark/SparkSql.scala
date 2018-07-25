package shiyaner
import org.apache.spark._ 
import org.apache.spark.SparkContext
import java.util.Properties 
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.Row 
/**
 * 编写spark程序，实现从mysql中读取DataFrame和向mysql中插入2条数据
 */
object SparkSql {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("SparkDataFrame").setMaster("local") 
    val sc = new SparkContext(conf) 
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //1.实现从mysql中读取DataFrame
     val jdbcDF = sqlContext.read.format("jdbc")
     .option("url", "jdbc:mysql://localhost:3306/spark")
     .option("driver","com.mysql.jdbc.Driver")
     .option("dbtable", "student")
     .option("user", "root").option("password", "hadoop").load()

     //2.向mysql中插入2条数据
     val studentRDD = sc.parallelize(
         Array("3 Rongcheng M 26","4 Guanhua M 27"))
         .map(_.split(" "))
     val schema = StructType(List(StructField("id", IntegerType, true),
         StructField("name", StringType, true),
         StructField("gender", StringType, true),
         StructField("age", IntegerType, true)))
     
     val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
     val studentDF = sqlContext.createDataFrame(rowRDD, schema)
     val prop = new Properties() 
     prop.put("user", "root")
     prop.put("password", "hadoop") 
     prop.put("driver","com.mysql.jdbc.Driver") 
     studentDF.write.mode("append")
     .jdbc("jdbc:mysql://localhost:3306/spark", "spark.student", prop)
     
  }
}