# Frequent Pattern Mining with Apache Spark

### Important

In order to successfully run the code, the worker nodes need to have the `spark-cassandra-connector_2.11-2.4.2.jar`  dependency in their classpath. For this, you should:

* Go to the [Maven Central](https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2) of the dependency
* Download the jar file
* Copy the downloaded jar file to the `/jars` directory in your Spark distribution

## Run the application

1. Build the application: `./gradlew shadowJar` or execute the `shadowJar` Gradle task in your IDE. This task will create a fatJar under the `pattern-analysis/build/libs` directory
2. Open up a terminal and submit the application: `/spark-2.4.5-bin-hadoop2.7/bin/spark-submit PATH_TO_PROJECT/pattern-analysis/build/libs/pattern-analysis-1.0-SNAPSHOT-all.jar`

For Elasticsearch, do the same. [Maven Centra](https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20_2.11/7.4.2)