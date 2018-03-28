package structured

import org.apache.log4j._

import java.util.Properties

import org.apache.kafka.clients.producer._

object Producer {

  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    println("Starting producer")
    
    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "producerTest"

    for (i <- 1 to 50) {
      val record = new ProducerRecord(TOPIC, "key", s"hello $i")
      producer.send(record)
    }

    val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
    producer.send(record)
    println("Sending finished")
    producer.close()
  }

}