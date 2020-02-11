FROM alpine:3.10

ENV SPARK_VERSION=2.4.5
ENV HADOOP_VERSION=2.7

RUN apk add --no-cache curl bash openjdk8-jre nss libc6-compat \
      && ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2 \
     # && chmod +x *.sh \
      && wget https://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && cd /
RUN apk --update add coreutils procps

# HERE WE ADD THE CASSANDRA AND ELASTICSEARCH CONNECTORS REQUIRED BY OUR ANALYSIS PROGRAM
ADD https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.4.2/spark-cassandra-connector_2.11-2.4.2.jar /spark/jars
ADD https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-20_2.11/7.4.2/elasticsearch-spark-20_2.11-7.4.2.jar /spark/jars

COPY master.sh /
COPY worker.sh /
COPY submit.sh /

# These concern the "master" node, i.e. if you start the container with /master.sh
ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs
ENV SPARK_MASTER_HOST `hostname`

# These concern the "worker" node, i.e. if you start the container with /worker.sh
ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

# These concern the "submit" task for submitting your JAR, i.e. if you start the container with /submit.sh
ENV SPARK_MASTER_NAME spark-master
ENV SPARK_APPLICATION_JAR_LOCATION /app/application.jar
ENV SPARK_APPLICATION_MAIN_CLASS my.main.Application
ENV SPARK_APPLICATION_ARGS ""

ENV SPARK_HOME /spark
# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
#ENV PYTHONHASHSEED 1
EXPOSE 8080 7077 6066 8081
CMD ["/bin/bash", "/master.sh"]
