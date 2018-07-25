package shiyaner
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.Row 
/**
 * 编写spark程序，使用利用反射机制推断RDD模式、
 * 编程方式定义RDD模式创建DataFrame。
 */
object SparkDataFrame {
  
  case class Person(name:String,age:Long)
  def main(args: Array[String]) { 
    //1.利用反射机制推断RDD模式创建DataFrame
    val conf = new SparkConf().setAppName("SparkDataFrame").setMaster("local") 
    val sc = new SparkContext(conf) 
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val people = sc.textFile("F:/demo/people.txt")
    val df = people.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    df.createOrReplaceTempView("people")
    val personsRDD = sqlContext.sql("select name,age from people where age > 20") 
    personsRDD.map(t => "Name:"+t(0)+","+"Age:"+ t(1)).show()
    
  
    //2.编程方式定义RDD模式创建DataFrame。
    val peopleRDD = sc.textFile("F:/demo/people.txt") 
     val schemaString = "name age"
     val fields = schemaString.split(" ").map(
         fieldName => StructField(fieldName, StringType, nullable = true)) 
     val schema = StructType(fields) 
     val rowRDD = peopleRDD.map(_.split(","))
     .map(attributes => Row(attributes(0), attributes(1).trim)) 
     val peopleDF = sqlContext.createDataFrame(rowRDD, schema) 
     peopleDF.createOrReplaceTempView("people")
     val results = sqlContext.sql("SELECT name,age FROM people")
     results.map(attributes => "name: " + attributes(0)+","+"age:"+attributes(1)).show() 
  }
}