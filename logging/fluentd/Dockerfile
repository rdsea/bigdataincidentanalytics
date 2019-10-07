FROM fluent/fluentd:v1.3.3-debian-1.0

USER root

RUN buildDeps="sudo make gcc g++ libc-dev ruby-dev" \
 && apt-get update \
 && apt-get install -y --no-install-recommends $buildDeps \
 && sudo gem install \
        fluent-plugin-elasticsearch \
 && sudo gem install \
         fluent-plugin-detect-exceptions \
 && sudo gem install \
         fluent-plugin-concat \
 && sudo gem install \
         fluent-plugin-grafana-loki \
 && sudo gem install \
         fluent-plugin-record-modifier --no-document \
 && sudo gem sources --clear-all \
 && SUDO_FORCE_REMOVE=yes \
    apt-get purge -y --auto-remove \
                  -o APT::AutoRemove::RecommendsImportant=false \
                  $buildDeps \
 && rm -rf /var/lib/apt/lists/* \
           /home/fluent/.gem/ruby/2.3.0/cache/*.gem

USER fluent