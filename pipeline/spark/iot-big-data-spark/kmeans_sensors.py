from __future__ import print_function

import time
import datetime
import os
import json
import uuid

from pyspark import SparkContext
from pyspark.ml.evaluation import ClusteringEvaluator
from fluent import asyncsender as sender
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.context import SQLContext
from pyspark.ml.clustering import KMeans

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
            'subject': record["device_id"] + record["time"],
            'message': 'Data read from HDFS',
            'data': record
        }
    )


if __name__ == "__main__":
    logger = sender.FluentSender(tag=tag, host=host, port=int(port), nanosecond_precision=True)
    sc = SparkContext(appName="KMeansSensorData",
                      master="spark://" + os.environ.get('SPARK_MASTER_NAME', default="spark-master") + ":"
                             + os.environ.get('SPARK_MASTER_PORT', default="7077"))  # SparkContext
    sqlContext = SQLContext(sc)
    # Load and parse the data
    try:
        data = sc.wholeTextFiles(hdfsUrl)

        rdd = sc.wholeTextFiles(hdfsUrl)
        for member in rdd.collect():
            json_rec = json.loads(member[1])
            process_record(json_rec)

        json_rdd = rdd.map(lambda x: json.loads(x[1]))
        df = sqlContext.read.json(json_rdd)
        vecAssembler = VectorAssembler(inputCols=["humidity", "temperature"], outputCol="features")
        new_df = vecAssembler.transform(df)

        # Build the model (cluster the data)
        k_means = KMeans(k=2, seed=1)
        model = k_means.fit(new_df.select('features'))

        predictions = model.transform(new_df)
        file_name = 'predictions_' + str(datetime.datetime.utcnow().isoformat("T") + "Z") + '.csv'
        predictions.write.csv('/spark/data/predictions/' + file_name)
        logger.emit_with_time('ml.app.dataAsset', time.time(), {
            'specversion': '0.3',
            'id': str(uuid.uuid4()),
            'type': tag + 'ml.app.dataAsset',
            'time': str(datetime.datetime.utcnow().isoformat("T") + "Z"),
            'source': 'Spark/KMeansSensorData(' + sc.applicationId + ')/cluster_data',
            'subject': file_name,
            'message': 'KMeans prediction written to file (' + file_name + ')'
        })

        # Evaluate clustering by computing Silhouette score
        evaluator = ClusteringEvaluator()

        silhouette = evaluator.evaluate(predictions)

        unique_id = str(uuid.uuid4())
        logger.emit_with_time('ml.app.dataAsset', time.time(), {
            'specversion': '0.3',
            'id': unique_id,
            'type': tag + 'ml.app.dataAsset',
            'time': str(datetime.datetime.utcnow().isoformat("T") + "Z"),
            'source': 'Spark/KMeansSensorData(' + sc.applicationId + ')/cluster_data',
            'subject': 'clustering_analysis_result_' + unique_id,
            'message': 'Within Set Sum of Squared Error computed',
            'data': {
                'silhouette_score': float(silhouette),
                'cluster_centers': model.clusterCenters()
            }
        })

        sc.stop()
        logger.close()
    except Exception as e:
        logger.emit_with_time(
            'ml.app.error', time.time(), {
                'specversion': '0.3',
                'id': str(uuid.uuid4()),
                'type': tag + 'ml.app.error',
                'source': 'Spark/KMeansSensorData(' + sc.applicationId + ')/main',
                'time': str(datetime.datetime.utcnow().isoformat("T") + "Z"),
                'subject': 'error',
                'message': 'ERROR during data analysis',
                'error': str(e)
            }
        )
        sc.stop()
        logger.close()
