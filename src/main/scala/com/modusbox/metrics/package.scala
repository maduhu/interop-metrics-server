package com.modusbox

import org.apache.kafka.common.serialization.StringDeserializer

package object metrics {
  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "ec2-35-164-199-6.us-west-2.compute.amazonaws.com:9092",
//    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "metric-backend",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

}
