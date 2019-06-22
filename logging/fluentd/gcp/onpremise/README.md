# TODO

* Document the requirement for credentials file in order for custom deployment to be able to ingest logs into Stackdriver.
These credentials MUST NOT be checked into the repository as they expose sensitive information.

* Also document why we have to build the Fluentd Docker images from scratch -> currently there is a version mismatch between fluentd and the GCP plugin. GCP plugin requires fluentd 1.14 and grpc 1.12; fluentd 1.14 is based on ruby 2.6.6. grpc 1.12 is only supported up to ruby 2.6 (not 2.6.6).
