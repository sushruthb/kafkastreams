package structured

import org.apache.spark._

import utils.{ KafkaServer, SimpleKafkaClient }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }


import org.apache.spark.sql._

import utils.{ KafkaServer, SimpleKafkaClient }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
object SendFile {
  
  def main(args:Array[String]){
    
    val topic = "MyFiles"

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
    
  }
  
}