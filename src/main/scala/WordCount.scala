/**
  * Created by mrllover on 22/03/2017.
  */

import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster("local")
    val sc = new SparkContext(conf)
    val pattern = """[^a-zA-Z]"""
    val file = "/Users/mrllover/Desktop/bible.txt"
    val files = "/Users/mrllover/Desktop/bible.txt,/Users/mrllover/Desktop/README.md"

    val wc = sc.textFile(files).
      flatMap(_.split(pattern)).
      map(_.trim).
      filter(!_.isEmpty).//remove empty elements
      map((_, 1)).
      reduceByKey(_+_).
      sortBy(_._1).
      sortBy(-_._2)
//      map{case (word, count) => (count, word)}.
//      sortByKey(false)
    wc.take(5).foreach(x=>println(x))
    wc.saveAsTextFile("wc.txt")

  }
}