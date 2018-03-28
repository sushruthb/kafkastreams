package structured
import java.util.Properties
import java.util
import org.apache.log4j._
import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
object ReadStreamFile {
  
   def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val topic="MyStream"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(topic))

    while (true) {
      val records = consumer.poll(50)
      for (record <- records.asScala) {
        println(record)
      }
    }
  }
  
}