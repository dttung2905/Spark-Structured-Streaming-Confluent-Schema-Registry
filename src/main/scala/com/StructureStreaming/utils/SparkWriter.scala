package com.StructureStreaming.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.config.ToAvroConfig


/**
 * Wrap the writestream function for various data sinks
 */
class SparkWriter {
  /**
   * Write to Kafka topic in Avro format
   * @param dataFrame: Spark Dataframe to be sinked to Kafka
   * @param kafkaProducerConfig: Contain option information
   * @param toAvroConfig: from ABRIS package to serialize the messages to be sent
   * @param interval: How long to start a trigger
   * @return: Streaming Query
   */
  def writeToKafkaAvro(dataFrame: DataFrame, kafkaProducerConfig: Map[String, String], toAvroConfig: ToAvroConfig, interval: String= "10 seconds"): StreamingQuery = {

    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
    dataFrame.select(to_avro(allColumns, toAvroConfig) as 'value)
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime(interval))
      .options(kafkaProducerConfig)
      .start()
  }

  /**
   * Weite to Kafka topic in Json format
   * @param dataFrame: Spark Dataframe to be sinked to Kafkae
   * @param kafkaProducerConfig: Contain option informationg
   * @param interval: How long to start a trigger
   * @return: Streaming Query
   */
  def writeToKafkaJson(dataFrame: DataFrame, kafkaProducerConfig: Map[String, String], interval: String="10 seconds"): StreamingQuery = {
    dataFrame
      .selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime(interval))
      .options(kafkaProducerConfig)
      .start()
  }
}
