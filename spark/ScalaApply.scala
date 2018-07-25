package shiyanyi

object ScalaApply {

  def main(args:Array[String]){
    //1.apply方法的运用
    println(apply("param1"))
    //apply方法初始化一个数组
    val myStr = Array("Hadoop","Spark","Hbase")
    //2.updata方法的运用
    //updata方法给数组赋值
    val myStrarr = new Array[String](3)
    myStrarr.update(0, "BigData")
    myStrarr.update(1, "Hive")
    myStrarr.update(2, "MapReduce")
  }
  //1.apply方法的运用
  def apply(param:String): String ={
    println("apply method called,parameter is :" + param)
    "Hello World!"
  } 
  

}