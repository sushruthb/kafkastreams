package structured

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }

import utils.{ KafkaServer, SimpleKafkaClient }
import java.util.Properties

import org.apache.log4j._
object StreamFile {
  def main(args: Array[String]) {

    import scala.io.Source
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val filename = "./src/main/resources/datafile.txt"
    
    var lineCount=0
    
    val topic="MyStream"
    
    
   
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    
 
    val producer=new KafkaProducer[String, String](props)
    
    for (line <- Source.fromFile(filename).getLines) {
      lineCount =lineCount+1
      println(line)
      val record=new ProducerRecord(topic, "key", "the end " + new java.util.Date) 
      
      producer.send(record);
    }
      
      producer.close()
            


  }

}