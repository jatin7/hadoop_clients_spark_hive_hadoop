package shiyanyi
import scala.collection.mutable.Map
object Scalamap {
  
  def main(args:Array[String]){
    //Map的创建
    val StudentMap = Map("quanyi"->"20151125","songqin"->"20151122")
    for((k,v) <- StudentMap)printf("姓名： %s 学号：%s\n",k,v);
    
    //Map的修改
    StudentMap("quanyi") = "2015081125" //修改匹配k的v
    println(StudentMap("quanyi"))
    
    //Map的新增
    StudentMap += ("zoudaifa"->"20151123") //添加一个新元素
    println(StudentMap("zoudaifa"))
  }
}