package com.xpandit.spark

import java.sql.Timestamp

import org.json.JSONObject

/**
  * Created by xpandit on 8/16/16.
  */
@SerialVersionUID(100L)
class TemperatureStateEvent(val rackId: Int, val time: Long, val temp: Double) extends Serializable {

  val HighTemperature = 40.0
  val RelevantTime = 120        //time window in seconds in which events will be considered from

  def highTemperature() = temp > HighTemperature
  def isTimeRelevant() = (System.currentTimeMillis() - time) <= RelevantTime * 1000

  def canEqual(a: Any) = a.isInstanceOf[TemperatureStateEvent]

  override def equals(that: Any): Boolean = {
    that match {
      case that: TemperatureStateEvent => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + rackId
    result = prime * result + time.hashCode()
    result = prime * result + temp.hashCode()
    return result
  }

  def toJSON() : JSONObject = {
    var json = new JSONObject()
    json.put("rackId", rackId)
    json.put("time", time)
    json.put("temp", temp)
  }

  override def toString(): String = "[ " + rackId + " , " + new Timestamp(time) + " , " + temp + " ]\n"
}
