package com.xpandit.spark

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import com.xpandit.utils.KafkaProducerHolder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable.Set


object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStatefulStreaming")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "250000")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/tmp/spark/checkpoint") // set checkpoint directory


    //Retrieving from Kafka
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "smallest")
    val topicsSet = scala.collection.immutable.Set("events")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val nonFilteredEvents = kafkaStream.map( (tuple) => createEvent(tuple._2) )

    val events = nonFilteredEvents.filter((pair) => {
      pair._2.highTemperature() && pair._2.isTimeRelevant()
    })

    //mapWithState function
    val updateState = (batchTime: Time, key: Int, value: Option[TemperatureStateEvent], state: State[(Option[Long], Set[TemperatureStateEvent])]) => {

      if (!state.exists) state.update((None, Set.empty))

      var updatedSet = Set[TemperatureStateEvent](value.get)

      //exclude non-relevant events
      state.get()._2.foreach((tempEvent) => {
        if (tempEvent.isTimeRelevant()) updatedSet.add(tempEvent)
      })

      var lastAlertTime = state.get()._1

      //launch alert if no alerts launched yet or if last launched alert was more than X seconds ago
      if (updatedSet.size >= 2 && (lastAlertTime.isEmpty || !timeNoMoreThanXseconds(lastAlertTime.get, 120))) {

        lastAlertTime = Some(System.currentTimeMillis())

        //alert in json to be published to kafka
        var json = new JSONObject
        json.put("time", lastAlertTime.get)

        var eventsArray = new JSONArray
        updatedSet.foreach( event => eventsArray.put(event.toJSON()) )

        json.put("events", eventsArray)

        //sending to kafka
        KafkaProducerHolder.producer.sendMessage("alerts", json.toString())

      }

      state.update((lastAlertTime, updatedSet))

      Some((key, updatedSet)) // mapped value

    }

    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = events.mapWithState(spec)

    mappedStatefulStream.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def createEvent(strEvent: String): (Int, TemperatureStateEvent) = {

    val eventData = strEvent.split('|')

    val time = eventData(0).toLong
    val rackId = eventData(1).toInt
    val temp = eventData(2).toDouble

    (rackId, new TemperatureStateEvent(rackId, time, temp))
  }

  def timeNoMoreThanXseconds(timestamp: Long, maxTimeDiffSeconds: Int): Boolean = {
    val diff = (System.currentTimeMillis() - timestamp)
    diff <= maxTimeDiffSeconds * 1000
  }
}