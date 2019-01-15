#!/usr/bin/env bash

# We're adding a "nifi" user and a corresponding "nifigroup"
addgroup nifigroup
adduser --disabled-password --gecos "" --ingroup nifigroup nifi

# After the HFDS is spun up, we create an HDFS for "nifi" and give the ownership to "nifi"
# (for seamless access to configuration resources later)
sleep 15 && hadoop fs -mkdir /nifi && hdfs dfs -chgrp nifigroup /nifi && hdfs dfs -chown nifi /nifi && echo "Hadoop for Nifi initialized" &

# this calls the original entrypoint script
exec ./run.sh