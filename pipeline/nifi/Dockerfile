FROM apache/nifi:1.10.0

#ADD https://github.com/mkjoerg/nifi-prometheus-reporter/releases/download/nifi-1.8.0/nifi-prometheus-nar-1.8.0.nar ${NIFI_BASE_DIR}/nifi-current/lib

# Add dependencies for log collection: Fluency, Logback more appenders + their dependencies
# These libraries need to be added to both "lib" and "bootstrap"
ADD http://central.maven.org/maven2/org/komamitsu/fluency/1.8.1/fluency-1.8.1.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/org/komamitsu/fluency/1.8.1/fluency-1.8.1.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD http://central.maven.org/maven2/com/sndyuk/logback-more-appenders/1.5.4/logback-more-appenders-1.5.4.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/com/sndyuk/logback-more-appenders/1.5.4/logback-more-appenders-1.5.4.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD http://central.maven.org/maven2/org/komamitsu/phi-accural-failure-detector/0.0.5/phi-accural-failure-detector-0.0.5.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/org/komamitsu/phi-accural-failure-detector/0.0.5/phi-accural-failure-detector-0.0.5.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD http://central.maven.org/maven2/org/msgpack/jackson-dataformat-msgpack/0.8.15/jackson-dataformat-msgpack-0.8.15.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/org/msgpack/jackson-dataformat-msgpack/0.8.15/jackson-dataformat-msgpack-0.8.15.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.9.7/jackson-databind-2.9.7.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.9.0/jackson-annotations-2.9.0.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.9.7/jackson-core-2.9.7.jar ${NIFI_BASE_DIR}/nifi-current/lib

ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/org/msgpack/msgpack-core/0.8.15/msgpack-core-0.8.15.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

ADD https://repo1.maven.org/maven2/io/cloudevents/cloudevents-api/0.2.1/cloudevents-api-0.2.1.jar ${NIFI_BASE_DIR}/nifi-current/lib
ADD https://repo1.maven.org/maven2/io/cloudevents/cloudevents-api/0.2.1/cloudevents-api-0.2.1.jar ${NIFI_BASE_DIR}/nifi-current/lib/bootstrap

USER root

# Setup NiFi user and create necessary directories
RUN chown -R nifi:nifi ${NIFI_BASE_DIR}/nifi-current/lib

USER nifi

