package com.StructureStreaming.utils

import com.typesafe.config.Config

/**
 * To get config from config.properties file
 */
class KafkaProducerConfig {
  def getConfig(config: Config): Map[String, String]= {
    val kafkaBootstrapServer: String = config.getString("kafka.bootstrap.servers")
    val offset: String = if(config.hasPath("offset")) config.getString("offset") else "latest"
    val schemaRegistryUrl: String = config.getString("schemaRegistry")
    val checkpointLocation: String = config.getString("checkpointLocation")
    val topic: String = config.getString("outputKafkaTopic")
    val acks: Option[Int] = if(config.hasPath("acks")) Some(Int.box(config.getInt("acks"))) else None
    val retry: Option[Int] = if(config.hasPath("retries")) Some(Int.box(config.getInt("retries"))) else None
    val bufferMemory: Option[Int] = if(config.hasPath("buffer.memory")) Some(Int.box(config.getInt("buffer.memory"))) else None
    val batchSize: Option[Int] = if(config.hasPath("batch.size")) Some(Int.box(config.getInt("batch.size"))) else None

    val outputMap : Map[String, Any] = Map(
      "kafka.bootstrap.servers" -> kafkaBootstrapServer,
      "offset" -> offset,
      "schema_registry_url" -> schemaRegistryUrl,
      "checkpointLocation" -> checkpointLocation,
      "acks" -> acks,
      "retry" -> retry,
      "buffer.memory" -> bufferMemory,
      "batch.size" -> batchSize,
      "topic" -> topic
    )
    outputMap.filter{case (k,v)=> v != None}.asInstanceOf[Map[String, String]]
  }
}
