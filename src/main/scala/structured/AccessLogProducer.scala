package structured

import org.apache.log4j._

import java.util.Properties

import org.apache.kafka.clients.producer._

object AccessLogProducer {

  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val filename = "./src/main/resources/access_log.txt"
    
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    println("Starting producer")
    
    val producer = new KafkaProducer[String, String](props)

    import scala.io.Source
    val TOPIC = "testLogs"
    val lineCount=0

     for (line <- Source.fromFile(filename).getLines) {
      
      println(line)
      val record=new ProducerRecord(TOPIC, "key", line) 
      
      producer.send(record);
     } 
   
    println("Sending finished")
    producer.close()
  }

}