package com.xpandit.spark

import _root_.kafka.serializer.StringDecoder
import com.xpandit.utils.KafkaProducerHolder
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable.Set


object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SparkStatefulStreaming")
      .setMaster("local[4]")
      .set("spark.driver.memory", "2g")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.backpressure.enabled", "true")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/tmp/spark/checkpoint") // set checkpoint directory


    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092, localhost:9093, localhost:9094, localhost:9095",
                                               "auto.offset.reset" -> "smallest")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, scala.collection.immutable.Set("events"))

    val nonFilteredEvents = kafkaStream.map((tuple) => createEvent(tuple._2))

    val events = nonFilteredEvents.filter((event) => {
      event.highTemperature() && event.isTimeRelevant()
    })

    val groupedEvents = events.transform((rdd) => rdd.groupBy(_.rackId))

    //mapWithState function
    val updateState = (batchTime: Time, key: Int, value: Option[Iterable[TemperatureStateEvent]], state: State[(Option[Long], Set[TemperatureStateEvent])]) => {

      if (!state.exists) state.update((None, Set.empty))

      var updatedSet = Set[TemperatureStateEvent]()

      value.get.foreach(updatedSet.add(_))

      //exclude non-relevant events
      state.get()._2.foreach((tempEvent) => {
        if (tempEvent.isTimeRelevant()) updatedSet.add(tempEvent)
      })

      var lastAlertTime = state.get()._1

      //launch alert if no alerts launched yet or if last launched alert was more than 120 seconds ago
      if (updatedSet.size >= 2 && (lastAlertTime.isEmpty || !((System.currentTimeMillis() - lastAlertTime.get) <= 120000))) {

        lastAlertTime = Some(System.currentTimeMillis())

        //alert in json to be published to kafka
        var json = new JSONObject
        json.put("time", lastAlertTime.get)

        var eventsArray = new JSONArray
        updatedSet.foreach(event => eventsArray.put(event.toJSON()))

        json.put("events", eventsArray)

        //sending to kafka
        KafkaProducerHolder.producer.sendMessage("alerts", json.toString())
      }

      state.update((lastAlertTime, updatedSet))

      Some((key, updatedSet)) // mapped value
    }

    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = groupedEvents.mapWithState(spec)

    mappedStatefulStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createEvent(strEvent: String): TemperatureStateEvent = {

    val eventData = strEvent.split('|')

    val time = eventData(0).toLong
    val rackId = eventData(1).toInt
    val temp = eventData(2).toDouble

    new TemperatureStateEvent(rackId, time, temp)
  }
}
