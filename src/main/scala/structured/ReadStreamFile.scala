package structured
import java.util.Properties
import java.util
import org.apache.log4j._
import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._ 
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent 
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
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
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("StreamProcessing")
    val kafkaParams = Map[String, Object]
                      ( "bootstrap.servers" -> "localhost:9092", 
                        "key.deserializer" -> classOf[StringDeserializer], 
                        "value.deserializer" -> classOf[StringDeserializer], 
                        "group.id" -> "use_a_separate_group_id_for_each_stream", 
                        "auto.offset.reset" -> "latest", 
                        "enable.auto.commit" -> (false: java.lang.Boolean) )
                        
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    val topics = Array("topicA")
    val stream = KafkaUtils.createDirectStream[String, String](
                ssc,  PreferConsistent,  Subscribe[String, String](topics, kafkaParams))
    
    
    
    
  }
  
}