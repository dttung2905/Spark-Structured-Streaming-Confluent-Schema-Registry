
package:
	sbt package

run:
	YOUR_SPARK_HOME/bin/spark-submit \
      --class "SimpleApp" \
      --master local[4] \
      target/scala-2.12/structuredstreaming_2.12-0.1.jar