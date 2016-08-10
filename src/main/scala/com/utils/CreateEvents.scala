package com.utils

object CreateEvents {

  def main(args: Array[String]): Unit = {

    val num_events_per_rack = 2
    val num_racks = 10000
    val outputPath = "/home/xpandit/Git/Spark-Streaming/src/main/resources/events.txt"

    val all_events = (0 until num_events_per_rack).foldLeft(new StringBuilder) { (sb, i) =>
      if (i > 0) sb.append('\n')
      generateEvent(num_racks, sb)
    }

    println(all_events)

    scala.tools.nsc.io.File(outputPath).writeAll(all_events.toString())

  }



  def generateEvent(numRacks: Int, sb: StringBuilder): StringBuilder = {

    for (rack <- 0 until numRacks) {

      val temperature = (Math.random() * 10) + 32   //to get high temperature once in a while

      sb.append(rack + "|" + "%.2f".format(temperature))

      if (rack != (numRacks - 1)) {
        sb.append('\n')
      }
    }
    sb
  }
}