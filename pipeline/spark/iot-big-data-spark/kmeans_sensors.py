from __future__ import print_function

import time
import datetime
import os
import json
import uuid
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
    logger.emit_with_time(
        'hdfs.app.dataAsset', time.time(), {
            'specversion': '0.3',
            'id': str(uuid.uuid4()),
            'type': tag + 'hdfs.app.dataAsset',
            'time': str(datetime.datetime.utcnow().isoformat("T") + "Z"),
            'source': 'Spark/KMeansSensorData(' + sc.applicationId + ')/process_record',
            'subject': record["id"],
            'log': 'Data read from HDFS',
            'data': record
        }
    )


if __name__ == "__main__":
    logger = sender.FluentSender(tag=tag, host=host, port=int(port), nanosecond_precision=True)
    sc = SparkContext(appName="KMeansSensorData",
                      master="spark://" + os.environ.get('SPARK_MASTER_NAME', default="spark-master") + ":"
                             + os.environ.get('SPARK_MASTER_PORT', default="7077"))  # SparkContext

    # Load and parse the data
    try:
        data = sc.wholeTextFiles(hdfsUrl)

        rdd = sc.wholeTextFiles(hdfsUrl)
        dataIds = ""
        provDerivedFrom = ""
        for member in rdd.collect():
            json_rec = json.loads(member[1])
            process_record(json_rec)

        json_rdd = rdd.map(lambda x: json.loads(x[1]))
        parsedData = json_rdd.map(lambda sensor_json: array([float(sensor_json["value"])]))

        # Build the model (cluster the data)
        clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")

        # Evaluate clustering by computing Within Set Sum of Squared Errors
        def error(point):
            center = clusters.centers[clusters.predict(point)]
            return sqrt(sum([x ** 2 for x in (point - center)]))


        WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
        logger.emit_with_time('ml.app.dataAsset', time.time(), {
            'specversion': '0.3',
            'id': str(uuid.uuid4()),
            'type': tag + 'ml.app.dataAsset',
            'time': str(datetime.datetime.utcnow().isoformat("T") + "Z"),
            'source': 'Spark/KMeansSensorData(' + sc.applicationId + ')/cluster_data',
            'subject': str(WSSSE),
            'log': 'Within Set Sum of Squared Error computed',
            'data': {
                'WSSSE': float(WSSSE)
            }
        })

        # Save and load model
        # clusters.save(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
        # sameModel = KMeansModel.load(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
        # $example off$

        sc.stop()
        logger.close()
    except Exception as e:
        logger.emit_with_time(
            'hdfs.app.error', time.time(), {
                'specversion': '0.3',
                'id': str(uuid.uuid4()),
                'type': tag + 'hdfs.app.error',
                'source': 'Spark/KMeansSensorData(' + sc.applicationId + ')/main',
                'time': str(datetime.datetime.utcnow().isoformat("T") + "Z"),
                'subject': 'error',
                'log': 'ERROR during data analysis',
                'error': str(e)
            }
        )
        sc.stop()
        logger.close()
