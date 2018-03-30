package rdds

import org.apache.log4j._
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

object WordCount {
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
   
    
    val conf=new SparkConf().setMaster("local[*]").setAppName("word count")
    
    val sc=new SparkContext(conf)
    
     //Create RDD from file.txt
    
    val rdd=sc.textFile("./src/main/resources/book.txt", 4)
    
    //Break each word in a line and count by value
    
    val words=rdd.flatMap(word=>word.split(" ")).countByValue()
    
    //take first 20 results
    
    val sample=words.take(20)
    
   
    //print them out
    
    for((word,count)<- sample){
      println(word+ " "+ count)
    }
    
    sc.stop
    
    
    
    
  }
  
}