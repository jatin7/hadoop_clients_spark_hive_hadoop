package shiyanyi

object ScalaIterable {
  
  def main(args:Array[String]){
    
    val iter = Iterator("Hadoop","Spark","Scala","Hive")
    /*while(iter.hasNext){
      println(iter.next())
    }*/
    //1.Iterable的grouped操作
    val git = iter grouped 2
    git.next;
    while(iter.hasNext){
      println(iter.next())
    }
    //2.Iterable的sliding操作
    val sit = iter sliding 2
    /*sit.next
    while(iter.hasNext){
      println(iter.next())
    }*/
  }
}