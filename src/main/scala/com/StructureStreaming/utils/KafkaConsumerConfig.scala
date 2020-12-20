package com.StructureStreaming.utils

import com.typesafe.config.Config

class KafkaConsumerConfig {
  def getConfig(config: Config): Map[String, String]= {
    val kafkaBootstrapServer: String = config.getString("kafka.bootstrap.servers")
    val topic: String = config.getString("inputKafkaTopic")

    val outputMap : Map[String, Any] = Map(
      "kafka.bootstrap.servers" -> kafkaBootstrapServer,
      "subscribe" -> topic
    )
    outputMap.filter{case (k,v)=> v != None}.asInstanceOf[Map[String, String]]
  }
}
