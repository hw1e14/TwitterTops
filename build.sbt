name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"

//libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.3"

libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.1.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.7"