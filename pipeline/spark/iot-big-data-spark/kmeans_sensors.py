from __future__ import print_function

import os
import json
from numpy import array
from math import sqrt

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from fluent import asyncsender as sender

tag = os.environ.get('FLUENTD_TAG_PREFIX', default="spark-kmeans")
host = os.environ.get('FLUENTD_HOST', default="localhost")
port = os.environ.get('FLUENTD_PORT', default="24224")
hdfsUrl = os.environ.get('HDFS_URL', default="hdfs://namenode:8020")


def process_record(record):
    logger.emit('hdfs', {
        'stage': 'input',
        'log': 'Data read from HDFS',
        'dataId': record["id"],
        'payload': record
    })


if __name__ == "__main__":
    logger = sender.FluentSender(tag=tag, host=host, port=int(port), nanosecond_precision=True)
    sc = SparkContext(appName="KMeansSensorData",
                      master="spark://" + os.environ.get('SPARK_MASTER_NAME', default="spark-master") + ":"
                             + os.environ.get('SPARK_MASTER_PORT', default="7077"))  # SparkContext

    # Load and parse the data
    data = sc.wholeTextFiles(hdfsUrl)
    rdd = data.values()
    dataIds = ""
    for member in rdd.collect():
        json_rec = json.loads(member)
        if dataIds:
            dataIds += ", " + json_rec["id"]
        else:
            dataIds += json_rec["id"]
        process_record(json_rec)

    json_rdd = rdd.map(lambda x: json.loads(x))
    parsedData = json_rdd.map(lambda sensor_json: array([float(sensor_json["value"])]))

    # Build the model (cluster the data)
    clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")

    # Evaluate clustering by computing Within Set Sum of Squared Errors
    def error(point):
        center = clusters.centers[clusters.predict(point)]
        return sqrt(sum([x ** 2 for x in (point - center)]))


    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    logger.emit('ml', {
        'stage': 'processing',
        'log': 'Within Set Sum of Squared Error computed',
        'dataId': dataIds,
        'WSSSE': float(WSSSE)
    })

    # Save and load model
    # clusters.save(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
    # sameModel = KMeansModel.load(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
    # $example off$

    sc.stop()
    logger.close()
