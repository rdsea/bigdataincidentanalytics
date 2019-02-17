# Apache Flink with Prometheus Exporter

The Dockerfile simply pulls the latest Flink image (1.7.1-alpine) 
and copies the JAR-file of the Prometheus Reporter to the `/lib`
directory as described in the 
[Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/metrics.html#prometheus-orgapacheflinkmetricsprometheusprometheusreporter).
Additionally it appends the required Prometheus flags to the `flink-conf.yml` file inside the container.