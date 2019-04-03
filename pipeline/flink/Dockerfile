FROM flink:1.7.2-alpine

# Remove Log4j libraries in order for Logback to take precedence as described in https://ci.apache.org/projects/flink/flink-docs-stable/dev/best_practices.html#use-logback-when-running-flink-on-a-cluster
RUN rm $FLINK_HOME/lib/log4j-1.2.*.jar
RUN rm $FLINK_HOME/lib/slf4j-log4j12-1.7.15.jar

# Add dependencies for log collection: Fluency, Logback more appenders + their dependencies
ADD http://central.maven.org/maven2/org/komamitsu/fluency/1.8.1/fluency-1.8.1.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/com/sndyuk/logback-more-appenders/1.5.4/logback-more-appenders-1.5.4.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/ch/qos/logback/logback-classic/1.2.3/logback-classic-1.2.3.jar $FLINK_HOME/lib/logback-classic.jar
ADD http://central.maven.org/maven2/ch/qos/logback/logback-core/1.2.3/logback-core-1.2.3.jar $FLINK_HOME/lib/logback-core.jar
ADD http://central.maven.org/maven2/org/komamitsu/phi-accural-failure-detector/0.0.5/phi-accural-failure-detector-0.0.5.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/org/msgpack/jackson-dataformat-msgpack/0.8.15/jackson-dataformat-msgpack-0.8.15.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.9.7/jackson-databind-2.9.7.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.9.0/jackson-annotations-2.9.0.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.9.7/jackson-core-2.9.7.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar $FLINK_HOME/lib
ADD http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.26/slf4j-api-1.7.26.jar $FLINK_HOME/lib/slf4j-api.jar
ADD http://central.maven.org/maven2/org/slf4j/log4j-over-slf4j/1.7.26/log4j-over-slf4j-1.7.26.jar $FLINK_HOME/lib/log4j-over-slf4j.jar

RUN echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"; \
    mv $FLINK_HOME/opt/flink-metrics-prometheus-1.7.2.jar $FLINK_HOME/lib

USER root

RUN chown -R flink:flink $FLINK_HOME/lib

USER flink