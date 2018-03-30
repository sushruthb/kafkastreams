name := "spark-kafka-streaming"

version := "1.0"

scalaVersion := "2.11.12"

fork := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0"

// Needed for structured streams

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.1"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"

//for Twitter streaming dependencies

// https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-twitter
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.2"


// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.3"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.3"

scalacOptions += "-target:jvm-1.8"



