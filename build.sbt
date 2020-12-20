name := "StructuredStreaming"

version := "0.1"

scalaVersion := "2.12.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-avro" % "3.0.1",
  "za.co.absa" % "abris_2.12" % "4.0.1",
  "io.confluent" % "kafka-avro-serializer" % "5.3.4",
  "com.typesafe" % "config" % "1.4.1"

)
resolvers += "confluent" at "https://packages.confluent.io/maven/"


