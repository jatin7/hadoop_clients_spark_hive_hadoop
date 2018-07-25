package shiyanyi

trait CarId{
  var id: Int
  def currentId(): Int
}

trait CarGreeting{
  def greeting(msg: String){
    println(msg)
  }
}

class BYDCarId extends CarId with CarGreeting{
  override var id = 10000
    def currentId(): Int = {id += 1;id}
}

class BMWCarId extends CarId with CarGreeting{
  override var id = 20000
    def currentId(): Int = {id += 1;id}
}

object Trait {
  def main(args:Array[String]){
    val myCarId1 = new BYDCarId()
    val myCarId2 = new BMWCarId()
    myCarId1.greeting("Welcome my first car.")
    println("my first CarId is %d.\n",myCarId1.currentId)
    myCarId2.greeting("Welcome my second car.")
    println("my second CarId is %d.\n",myCarId2.currentId)
  }
}