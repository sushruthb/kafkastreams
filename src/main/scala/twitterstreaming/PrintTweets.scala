package twitterstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j._



object PrintTweets {
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    val ssc=new StreamingContext("local[*]","printtweets", Seconds(1))
    
    //val tweets=TwitterUtils.createStream(ssc,None)
    
    
  }
  
}