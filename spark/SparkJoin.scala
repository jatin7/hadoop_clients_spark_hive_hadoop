package shiyaner
import org.apache.spark._ 
import SparkContext._ 
/**
 * 连接操作
 */
object SparkJoin {
  def main(args:Array[String]) { 
    if (args.length != 3 ){ 
      println("usage is WordCount <rating> <movie> <output>") 
      return 
    } 
    val conf = new SparkConf().setAppName("SparkJoin").setMaster("local") 
    val sc = new SparkContext(conf) 
    // Read rating from HDFS file 
    val textFile = sc.textFile(args(0)) 
    //extract (movieid, rating) 
    val rating = textFile.map(line => { 
      val fileds = line.split("::") 
      (fileds(1).toInt, fileds(2).toDouble) 
      }) 
     //get (movieid,ave_rating) 
     val movieScores = rating .groupByKey() .map(data => { 
        val avg = data._2.sum / data._2.size 
        (data._1, avg) 
      })
     // Read movie from HDFS file 
      val movies = sc.textFile(args(1)) 
      val movieskey = movies.map(line => { 
        val fileds = line.split("::") 
        (fileds(0).toInt, fileds(1)) 
        //(MovieID,MovieName) 
        }).keyBy(tup => tup._1) 
        // by join, we get <movie, averageRating, movieNa me> 
        val result = movieScores
        .keyBy(tup => tup._1)
        .join(movieskey)
        .filter(f => f._2._1._2 > 4.0)
        .map(f => (f._1, f._2._1._2, f._2._2._2)) 
        result.saveAsTextFile(args(3)) 
  }
}