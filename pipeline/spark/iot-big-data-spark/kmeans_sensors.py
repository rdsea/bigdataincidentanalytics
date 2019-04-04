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
    logger.emit('hdfs.app.dataAsset', {
        'log': 'Data read from HDFS',
        'payload': record
    })


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
            if dataIds:
                dataIds += ", " + json_rec["id"]
            else:
                dataIds += json_rec["id"]
                provDerivedFrom = json_rec["prov"]["id"]
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
        logger.emit('ml.app.dataAsset', {
            'log': 'Within Set Sum of Squared Error computed',
            'data': {
                'WSSSE': float(WSSSE),
                'prov': {
                    'id': 'spark',
                    'wasDerivedFrom': provDerivedFrom,
                    'type': 'calculatedValue',
                    'wasGeneratedBy': 'spark-kmeans-ml'
                }
            }
        })

        # Save and load model
        # clusters.save(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
        # sameModel = KMeansModel.load(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
        # $example off$

        sc.stop()
        logger.close()
    except Exception as e:
        logger.emit('hdfs.app.error', {
            'log': 'ERROR cannot read from HDFS' + hdfsUrl,
            'error': str(e)
        })
        sc.stop()
        logger.close()
