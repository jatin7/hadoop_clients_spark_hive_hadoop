package shiyanyi
import java.io.File
import scala.io.Source
object WordCount {
  def main(args:Array[String]){
    val dirfile = new File("G://demo")
    val files = dirfile.listFiles
    for(file <- files)println(file)
    val listFiles = files.toList
    val wordsMap = scala.collection.mutable.Map[String,Int]()
    listFiles.foreach(file => Source.fromFile(file).getLines().
        foreach(line =>line.split("").foreach(
            word=>{
              if (wordsMap.contains(word)){
                wordsMap(word)+=1
              }else{
                wordsMap+=(word->1)
              }
            })))
        println(wordsMap)
        for((key,value) <- wordsMap)println(key+": "+value)
  }
}