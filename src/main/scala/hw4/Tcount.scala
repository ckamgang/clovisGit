package hw4
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object Tcount {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    //wordcount example
    val sc = new SparkContext("local[*]", "SparkDemo01")
    val lines = sc.textFile("T.txt")
    val words = lines.flatMap(line => line.split(' '))
    val wordsKVRdd = words.map(x => (x, 2))
    val count = wordsKVRdd.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)

    count.foreach(println)
  }
}
