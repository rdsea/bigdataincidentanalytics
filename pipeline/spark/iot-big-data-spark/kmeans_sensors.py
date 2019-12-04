from __future__ import print_function

import time
import datetime
import os
import json
import uuid
import pandas as pd

from pyspark import SparkContext, RDD
from pyspark.ml.evaluation import ClusteringEvaluator
from fluent import asyncsender as sender
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.context import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import IntegerType, TimestampType

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


def check_data_quality(json_rdd_s: RDD):
    for member in json_rdd_s.collect():
        process_record(member)


if __name__ == "__main__":
    logger = sender.FluentSender(tag=tag, host=host, port=int(port), nanosecond_precision=True)
    sc = SparkContext(appName="KMeansSensorData",
                      master="spark://" + os.environ.get('SPARK_MASTER_NAME', default="spark-master") + ":"
                             + os.environ.get('SPARK_MASTER_PORT', default="7077"))
    sqlContext = SQLContext(sc)

    try:
        # Load data
        rdd = sc.wholeTextFiles(hdfsUrl)

        # Parse each entry to a valid JSON
        json_rdd = rdd.map(lambda x: json.loads(x[1]))

        # Perform data quality checks, logging of events
        check_data_quality(json_rdd)

        # Create DataFrame with proper types
        df = sqlContext.read.json(json_rdd)
        df = df.withColumn("humidity", df["humidity"].cast(IntegerType()))
        df = df.withColumn("temperature", df["temperature"].cast(IntegerType()))
        df = df.withColumn("time", df["time"].cast(TimestampType()))

        # Create a target vector for clustering. In this case we combine the humidity and temperature values
        vecAssembler = VectorAssembler(inputCols=["humidity", "temperature"], outputCol="features")
        new_df = vecAssembler.transform(df)

        # Build the model (cluster the data)
        k_means = KMeans(k=2, seed=1)
        model = k_means.fit(new_df.select('features'))

        # Get the predictions
        predictions = model.transform(new_df)

        # Save the predictions to a local .CSV file for debugging purposes
        file_name = 'predictions_' + str(datetime.datetime.utcnow().isoformat("T") + "Z").replace('-', '_')\
                    .replace(':', '_')\
                    .replace('.', '_') + '.csv'
        predictions.sort(["time"]).toPandas().to_csv("/spark/data/predictions/" + file_name, index=False)

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
            'message': 'KMeans Silhouette Score and Cluster Centers computed (AnalysisId:' + unique_id + ")",
            'data': {
                'silhouette_score': float(silhouette),
                'cluster_centers': pd.Series(model.clusterCenters()).to_json(orient='values')
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
