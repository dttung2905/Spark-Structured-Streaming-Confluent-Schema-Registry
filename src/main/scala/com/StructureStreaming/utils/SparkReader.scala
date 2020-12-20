package com.StructureStreaming.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.FromAvroConfig

/**
 * Wrap the readStream function for various data sources
 */
class SparkReader {
  /**
   * read messages from Kafka in Avro format and explode into multiple columns
   * @param sparkSession: Spark sparkSession
   * @param consumerConfig: Contain option information
   * @param fromAvroConfig: from ABRIS package to deserialize the messages to be read/ consumed
   * @return: Spark Dataframe
   */
  def readKafkaAvro(sparkSession: SparkSession, consumerConfig: Map[String, String], fromAvroConfig: FromAvroConfig): DataFrame = {
    assert (consumerConfig.contains("kafka.bootstrap.servers"))
    assert (consumerConfig.contains("subscribe"))

    val df = sparkSession
      .readStream
      .format("kafka")
      .options(consumerConfig)
      .load()
    df.select(from_avro(col("value"), fromAvroConfig) as 'data).select("data.*")
  }

  /**
   * read messages from socket
   * @param sparkSession: Spark sparkSession
   * @param consumerConfig: Contain option information
   * @return: Spark Dataframe
   */
  def readSocket(sparkSession: SparkSession,consumerConfig: Map[String, String] ): DataFrame ={
    assert( consumerConfig.contains("host"))
    assert( consumerConfig.contains("port"))

    val df = sparkSession
      .readStream
      .format("socket")
      .options(consumerConfig)
      .load()
    df
  }
}
