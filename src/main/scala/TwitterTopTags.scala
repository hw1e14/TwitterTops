/**
  * Created by mrllover on 23/03/2017.
  */

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._


object TwitterTopTags {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()
    val config = new SparkConf().setAppName("twitter-stream-top-tags")
            .setMaster("local[2]")
    val sc = new SparkContext(config)
    sc.setLogLevel("ERROR")
    //    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(5))

    System.setProperty("twitter4j.oauth.consumerKey", conf.getString("dev.twitter.consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", conf.getString("dev.twitter.consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", conf.getString("dev.twitter.accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", conf.getString("dev.twitter.accessTokenSecret"))


    val outputDirectory = "/twitter"

    val slideInterval = new Duration(1 * 1000)

    val windowLength = new Duration(5 * 1000)

    val timeoutJobLength = 100 * 1000

    val stream = TwitterUtils.createStream(ssc, None)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//        .transform(_.sortBy(-_._2))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//      .transform(_.sortBy(-_._2))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()

  }

}