package structured

import utils.{ KafkaServer, SimpleKafkaClient }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.spark.sql._
object Simple {
  def main(args: Array[String]) {

    val topic = "foo"

    println("Starting Kafka server")
    val kafkaServer = new KafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    Thread.sleep(5000)

    // publish some messages
    println("*** Publishing messages")
    val max = 5
    val client = new SimpleKafkaClient(kafkaServer)
    val numbers = 1 to max
    val producer = new KafkaProducer[String, String](client.basicStringStringProducer)
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic, "[1]key_" + n, "[1]string_" + n))
    }
    
    Thread.sleep(5000)
     println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("Structured_Simple")
      .config("spark.master", "local[4]")
      .getOrCreate()

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // equivalent of auto.offset.reset which is not allowed here
      .load()
      
     val counts = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val query = counts.writeStream
      .format("console") // write all counts to console when updated
      .start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    println("*** publishing more messages")
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic, "[2]key_" + n, "[2]string_" + n))
    }

    Thread.sleep(5000)

    println("*** Stopping stream")
    query.stop()

    query.awaitTermination()
    spark.stop()

    println("*** Streaming terminated")

    // stop Kafka
    println("*** Stopping Kafka")
    kafkaServer.stop()

    println("*** done")

  }
}