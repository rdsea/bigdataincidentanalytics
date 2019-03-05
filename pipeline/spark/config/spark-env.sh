#!/usr/bin/env bash
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export HADOOP_CONF_DIR=/etc/hadoop
export PYSPARK_PYTHON=/usr/bin/python3
pip3 install numpy
