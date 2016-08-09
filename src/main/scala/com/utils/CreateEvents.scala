package com.utils

import java.sql.{Date, Time, Timestamp}

object CreateEvents {

  final val MAX_TEMPERATURE = 100
  final val NORMAL_MAX_TEMPERATURE = 40
  final val MIN_TEMPERATURE = 18


  def generateEvent(i: Int, timestamp: Long, numRacks: Int, sb: StringBuilder): StringBuilder = {
    val time = timestamp + i * 30000 //each 30 seconds

    //random high temperature event
    val high_temperature = if ((Math.random() * 10).toInt == 5) true else false
    val rack_to_heat = (Math.random() * numRacks).toInt

    for (rack <- 0 until numRacks) {
      val temperature = if (rack == rack_to_heat && high_temperature) {
        val t = NORMAL_MAX_TEMPERATURE + (Math.random() * ((MAX_TEMPERATURE - NORMAL_MAX_TEMPERATURE) + 1)).toInt //getting random high temperature
        println(s"HIGH TEMPERATURE: $t ON RACK: $rack")
        t
      }
      else {
        MIN_TEMPERATURE + (Math.random() * ((NORMAL_MAX_TEMPERATURE - MIN_TEMPERATURE) + 1)).toInt //getting random fine temperature
      }

      sb.append(time + "|" + rack + "|" + temperature)

      if (rack != (numRacks - 1)) {
        sb.append('\n')
      }
    }
    sb
  }

  def main(args: Array[String]): Unit = {

    val num_events = 10
    val num_racks = 3
    val timestamp = 1369786953 * 1000.toLong    //change initial timestamp here

   // print(time)

    val all_events = (0 until num_events).foldLeft(new StringBuilder) { (sb, i) =>
      if (i > 0) sb.append('\n')
      generateEvent(i, timestamp, num_racks, sb)
    }


    println(all_events)
  }
}