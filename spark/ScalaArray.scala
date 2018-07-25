package shiyanyi

object ScalaArray {
  def main(args:Array[String]){
    val myarr = Array("Hadoop","Spark","Hbase")
    //1.数组的map操作
    myarr.map(s => s.toUpperCase()).foreach(println);
    //2.数组的foreach操作
    myarr.foreach(println)
    //3.数组的filter操作
    myarr.filter(s => true).foreach(println)
  }
}