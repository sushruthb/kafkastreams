package structured

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import utils.{ KafkaServer, SimpleKafkaClient }

object StreamFile {
  def main(args: Array[String]) {

    import scala.io.Source
    
    

    val filename = "./src/main/resources/datafile.txt"
    
    var lineCount=0
    
    val topic="MyTopic"
    
    
    println("Starting Kafka server")
    val kafkaServer = new KafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    Thread.sleep(5000)
    
    val client = new SimpleKafkaClient(kafkaServer)
    val producer = new KafkaProducer[String, String](client.basicStringStringProducer)
    
    for (line <- Source.fromFile(filename).getLines) {
      lineCount =lineCount+1
      println(line)
    //cd c  producer.sendMessage(new ProducerRecord(topic, lineCount + line ));
      
            
    }

  }

}