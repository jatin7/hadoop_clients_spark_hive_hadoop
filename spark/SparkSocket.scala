package shiyaner
import org.apache.spark._ 
import org.apache.spark.streaming._ 
import org.apache.spark.storage.StorageLevel 
/**
 * 编写spark程序，利用Spark Streaming对Socket端口监听并接收数据，然后进行相应处理
 */
object SparkSocket {
def main(args: Array[String]) { 
  if (args.length < 2) { 
    System.err.println("Usage: NetworkWordCount <hostname> <port>") 
    System.exit(1) 
    } 
  val sparkConf = new SparkConf().setAppName("SparkSocket").setMaster("local") 
  val ssc = new StreamingContext(sparkConf, Seconds(1)) 
  val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER) 
  val words = lines.flatMap(_.split(" ")) 
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _) 
  wordCounts.print() 
  ssc.start() 
  ssc.awaitTermination()
  }
}
