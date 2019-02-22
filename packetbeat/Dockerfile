ARG ELK_VERSION

FROM docker.elastic.co/beats/packetbeat:${ELK_VERSION}
COPY packetbeat.yml /usr/share/packetbeat/packetbeat.yml
USER root
RUN chown packetbeat /usr/share/packetbeat/packetbeat.yml
USER packetbeat