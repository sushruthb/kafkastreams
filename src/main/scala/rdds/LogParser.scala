package rdds

import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.log4j._
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

object LogParser {
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //Create the streaming context with 1 second batch
    val ssc=new StreamingContext("local[*]","Log Parser",Seconds(1))
    
    //Construct a reqular expression to extract fields from apache log lines
    
    val pattern=apacheLogPattern()
    
    //Create a socket stream to read log data via nc on port 9999
    val lines=ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    //Extract the request field from each log line
    
    val requests=lines.map(x=>{ val matcher:Matcher=pattern.matcher(x);if(matcher.matches()) matcher.group(5)})
    
    //Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    
    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    
    // Kick it off
    ssc.checkpoint("/home/hdfs/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
  def apacheLogPattern():Pattern = {
   val ddd = "\\d{1,3}"                      
   val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
   val client = "(\\S+)"                     
   val user = "(\\S+)"
   val dateTime = "(\\[.+?\\])"              
   val request = "\"(.*?)\""                 
   val status = "(\\d{3})"
   val bytes = "(\\S+)"                     
   val referer = "\"(.*?)\""
   val agent = "\"(.*?)\""
   val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
   Pattern.compile(regex)    
 }
  
}