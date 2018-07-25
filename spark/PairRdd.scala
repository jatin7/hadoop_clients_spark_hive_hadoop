package shiyaner
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
/**
 * 1.使用textFile和parallelize创建Pair RDD 
 * 2.RDD进groupByKey、reduceByKey的操作
 * 
 */
object PairRdd {
  
  def main(args:Array[String]){
    //1.使用textFile创建Pair RDD
    val conf = new SparkConf()
    
    conf.setMaster("local")
    conf.setAppName("PairRdd")
    val sc = new SparkContext(conf)
    val tfile = sc.textFile("‪F:/demo/word.txt");
    val tmap = tfile.map(word => (word,1))
    //2.使用parallelize创建Pair RDD
    val list = List("asdasd","dfhdfh","sdfgdfg");
    val pfile = sc.parallelize(list);
    val pmap = pfile.map(word => (word,1))
    //reduceByKey操作
    tmap.reduceByKey((a,b)=>a+b).foreach(println)
    pmap.reduceByKey((a,b)=>a+b).foreach(println)
    
    //groupBykey操作
    tmap.groupByKey().foreach(println)
    pmap.groupByKey().foreach(println)
    
  }
}