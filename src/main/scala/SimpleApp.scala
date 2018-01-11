import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mrllover on 08/06/2017.
  */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "/usr/local/spark/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
