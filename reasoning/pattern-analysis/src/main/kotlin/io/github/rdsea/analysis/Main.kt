package io.github.rdsea.analysis

import com.datastax.spark.connector.japi.CassandraJavaUtil.column
import com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions
import com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_distinct
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.size
import org.apache.spark.sql.functions.sort_array
import org.apache.spark.sql.functions.window
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark.saveJsonToEs

/**
 * <h4>About this class</h4>
 *
 * <p>Description</p>
 *
 * @author Daniel Fuevesi
 * @version 1.0.0
 * @since 1.0.0
 */
class Main {

    companion object {
        private val MASTER_URL = System.getenv("SPARK_MASTER_URL") ?: "spark://analysis-spark-master:7077"
        private val CASSANDRA_HOST = System.getenv("APP_CASSANDRA_HOST") ?: "cassandra"
        private val ELASTICSEARCH_HOST = System.getenv("APP_ELASTICSEARCH_HOST") ?: "elasticsearch"
        private val ELASTICSEARCH_PORT = System.getenv("APP_ELASTICSEARCH_PORT") ?: "9200"
        private val FP_MIN_SUPPORT = System.getenv("APP_FP_MIN_SUPPORT") ?: "0.1"
        private val FP_MIN_CONFIDENCE = System.getenv("APP_FP_MIN_CONFIDENCE") ?: "0.5"
        private val WINDOW_DURATION = System.getenv("APP_WINDOW_DURATION") ?: "1 minute"
        private val SLIDE_DURATION = System.getenv("APP_SLIDE_DURATION") ?: "0 second"
        private val GSON: Gson = GsonBuilder()
            .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeJsonSerializer())
            .create()

        @JvmStatic fun main(args: Array<String>) {
            val conf = SparkConf(true)
                .set("spark.cassandra.connection.host", CASSANDRA_HOST)
                .set("spark.sql.session.timeZone", "UTC")
                .set("es.nodes", ELASTICSEARCH_HOST)
                .set("es.port", ELASTICSEARCH_PORT)
                .set("es.index.auto.create", "true")
                .set("es.nodes.discovery", "true")
            val sc = SparkContext(MASTER_URL, "Frequent Pattern Mining", conf)
            sc.setLogLevel("WARN")
            val sparkSession = SparkSession.builder().sparkContext(sc).orCreate

            // Read data from Cassandra and map each record to a simplified one suitable for FP-Mining
            val rdd: JavaRDD<Item> = readRecordsFromCassandra(sc)
                .map { Item("${it.name}#${it.pipelineComponent}", it.timestamp) }

            // Assign items to (sliding) time windows (regulated by WINDOW_DURATION, SLIDE_DURATION) and collect them to a list
            // Example row: time-window1: [SignalName1, SignalName2, SignalName3]
            val dataFrame = sparkSession.createDataFrame(rdd, Item::class.java)
                .select("*")
                .groupBy(window(Column("timestamp"), WINDOW_DURATION, SLIDE_DURATION))
                .agg(collect_list("signalId"))
                .orderBy(col("window"))
                .withColumn("signals_set", array_distinct(col("collect_list(signalId)")))
                .drop("window", "collect_list(signalId)")

            // Perform FP-Mining via Spark MLlib's FPGrowth algorithm
            val model = FPGrowth()
                .setItemsCol("signals_set")
                .setMinSupport(FP_MIN_SUPPORT.toDouble())
                .setMinConfidence(FP_MIN_CONFIDENCE.toDouble())
                .fit(dataFrame)

            // Optional: Print Intermediary output of frequent item sets
            model.freqItemsets().show(false)

            // Optional: Print Intermediary output of derived association rules
            model.associationRules().show(false)

            // Get predictions on the original dataset.
            // Empty predictions are filtered out and duplicate ones removed.
            val predictions = model.transform(dataFrame)
                .filter(size(col("prediction")).`$greater`(0)) // filter out empty predictions
                .withColumn("signals_set", sort_array(col("signals_set"))) // sort each signal list alphabetically so that we can remove duplicates that are caused by permutation
                .dropDuplicates()

            // Optional: Print predictions to the console
            predictions.show(false)

            val predictionsRDD = dataFrameToPredictionsRDD(predictions)
            // Store the predictions in Elasticsearch for delivery, where operators can get notified about them
            savePredictionsToEs(predictionsRDD)
        }

        private fun readRecordsFromCassandra(sparkContext: SparkContext): JavaRDD<SignalRecord> {
            return javaFunctions(sparkContext)
                .cassandraTable("incident_analytics", "signals", mapRowTo(SignalRecord::class.java))
                .select(
                    column("uuid"),
                    column("signal_name").`as`("name"),
                    column("pipeline_component").`as`("pipelineComponent"),
                    column("timestamp")
                )
        }

        private fun dataFrameToPredictionsRDD(dataset: Dataset<Row>): JavaRDD<Prediction> {
            val analysisTimestamp = LocalDateTime.now(ZoneOffset.UTC)
            val analysisId = UUID.randomUUID().toString()
            return dataset
                .toJavaRDD()
                .map { Prediction(analysisId, analysisTimestamp, it.getList(0), it.getList(1)) }
        }

        private fun savePredictionsToEs(predictions: JavaRDD<Prediction>) {
            val jsonRdd = predictions.map { GSON.toJson(it) }
            saveJsonToEs(jsonRdd, "correlated-signal-predictions")
        }
    }
}
