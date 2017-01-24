package com.spark.sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
import scala.math.max
import scala.math.min



object projectcode {
   
  val sc = new SparkConf().setAppName("FlumePollingEventCount")
  val ssc = new StreamingContext(sc, Seconds(10))
  val stream = FlumeUtils.createPollingStream(ssc, "master01", 9999)
  val lines = stream.map(x => new String(x.event.getBody().array()))
  
  val filter_total = lines.filter(x =>(!(x.toString().contains("Total"))))
  val linesMap = filter_total.map(x => x.toString().split(":"))
  val desc = filter_total.map(x => x.toString().split(":")(1).distinct)
  val reading = filter_total.map(x => x.toString().split(":")(2).toFloat)
  val i_tag = filter_total.map(x => x.toString().split(":")(3))
  val timestamp = filter_total.map(x => x.toString().split(":")(4))
  
//  lines.count().map(cnt => "Received " + cnt + " events." ).print()
  

  val mapDescItag = filter_total.map(x => (x.toString().split(":")(1),x.toString().split(":")(3).toFloat))
  
 val successFailure = i_tag.map(x => {
      val i = x.toFloat
      if (i < 0) {
        "Success"
      } else if (i > 0) {
        "Failure"
      } else {
        "Other"
      }
    })
    val statusCounts=successFailure.countByValueAndWindow(Seconds(10), Seconds(10))
    
    statusCounts.foreachRDD((rdd, time) => {
      // Keep track of total success and error codes from each RDD
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }

      // Print totals from current window
      println("Total success: " + totalSuccess + " Total failure: " + totalError)
    })
//max and min value for every meter
  
  val mapDescRead = filter_total.map(x => (x.toString().split(":")(2).toFloat,x.toString().split(":")(1)))
  println("test")
  //  val reduceDescRead = mapDescRead.reduceByKey((x,y) => max(x,y))
  mapDescRead.foreachRDD(x => if(!x.isEmpty())(println("The max meter reading is : " + x.max)))
  mapDescRead.foreachRDD(x => if(!x.isEmpty())(println("The min meter reading is : " + x.min)))  
    
  //Checking individual panel status
  val statusdescI_tag = mapDescItag.map(x =>  if (x._2 < 0) (x._1,"up") else (x._1,"down"))
  statusdescI_tag.print()

  
// To print max or min value of each panel
//  val reducemap = reduceDescRead.map(x=> (x,x._2))
  
//  val maxvalue = reduceDescRead.foreachRDD(x => print(x.max))
//  val minvalue = reduceDescRead.foreachRDD(x => print(x.min))
  

//  reduceDescRead.print()

//lines.print()
ssc.checkpoint("/home")
ssc.start()
ssc.awaitTermination()

}
