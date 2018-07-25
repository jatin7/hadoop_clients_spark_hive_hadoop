package shiyanyi

object Generator {
  
  def main(args:Array[String]){
    //scala的for循环
    //1.一个生成器
    for (i <- 1 to 5) println(i)
    
    //2.多个生成器
    for (i <- 1 to 5; j <- 6 to 10) println(i+j)
    
    //3.一个生成器的守卫模式
    for (i <- 1 to 5 if i%2==0) println(i)
    
    //4.多个生成器的守卫模式
    for (i <- 1 to 5 if i%2==0; j <- 1 to 3 if j!= i) println(i*j)
  }
  
}