FROM bde2020/spark-worker:2.4.0-hadoop2.8

RUN rm /opt/hadoop-2.8.0/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar

# Add dependencies for log collection: Fluency, Logback more appenders + their dependencies
ADD http://central.maven.org/maven2/org/komamitsu/fluency/1.8.1/fluency-1.8.1.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/com/sndyuk/logback-more-appenders/1.5.4/logback-more-appenders-1.5.4.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/ch/qos/logback/logback-classic/1.2.3/logback-classic-1.2.3.jar $SPARK_HOME/jars/logback-classic.jar
ADD http://central.maven.org/maven2/ch/qos/logback/logback-core/1.2.3/logback-core-1.2.3.jar $SPARK_HOME/jars/logback-core.jar
ADD http://central.maven.org/maven2/org/komamitsu/phi-accural-failure-detector/0.0.5/phi-accural-failure-detector-0.0.5.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/org/msgpack/jackson-dataformat-msgpack/0.8.15/jackson-dataformat-msgpack-0.8.15.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.9.7/jackson-databind-2.9.7.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.9.0/jackson-annotations-2.9.0.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.9.7/jackson-core-2.9.7.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.26/slf4j-api-1.7.26.jar $SPARK_HOME/jars/slf4j-api.jar
ADD http://central.maven.org/maven2/org/slf4j/log4j-over-slf4j/1.7.26/log4j-over-slf4j-1.7.26.jar $SPARK_HOME/jars/log4j-over-slf4j.jar

