package com.StructureStreaming

import com.StructureStreaming.utils.{KafkaConsumerConfig, KafkaProducerConfig, SparkReader, SparkWriter}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{ Dataset, Row, SparkSession}
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}



object SimpleApp2 {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load(args(0))
    val inputKafkaTopic: String = config.getString("inputKafkaTopic")
    val outputKafkaTopic: String = config.getString("outputKafkaTopic")
    val schemaRegistry: String = config.getString("schemaRegistry")
    val sparkMaster: String = config.getString("spark.master")
    val outputKafkaSchema: String = config.getString("outputKafkaSchema")
    val sparkSession = SparkSession.builder.appName("Simple Application").config("spark.master", sparkMaster).getOrCreate()
    sparkSession.sparkContext.setLogLevel("INFO")
    import sparkSession.implicits._
    val kafkaConsumerConfig: Map[String, String] = new KafkaConsumerConfig().getConfig(config)

    val fromAvroConfig: FromAvroConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(inputKafkaTopic) // Use isKey=true for the key schema and isKey=false for the value schema
      .usingSchemaRegistry(schemaRegistry)
    val sparkReader = new SparkReader()
    val expandedDf = sparkReader.readKafkaAvro(sparkSession, kafkaConsumerConfig, fromAvroConfig)
    val avroDfTransformed: Dataset[Row] = expandedDf.select(expandedDf.columns.head, expandedDf.columns.tail: _*).where("previous_state != ''")


    val toAvroConfig: ToAvroConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(outputKafkaSchema.stripMargin)
      .usingTopicNameStrategy(outputKafkaTopic) // name and namespace taken from schema
      .usingSchemaRegistry(schemaRegistry)
    val sparkWriter = new SparkWriter()
    val kafkaProducerConfig: Map[String, String] = new KafkaProducerConfig().getConfig(config)
    sparkWriter
      .writeToKafkaAvro(avroDfTransformed, kafkaProducerConfig, toAvroConfig, "20 seconds")
      .awaitTermination()
  }
}
