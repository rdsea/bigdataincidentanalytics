package io.github.rdsea.analysis

import com.datastax.spark.connector.japi.CassandraJavaUtil.column
import com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions
import com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_distinct
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.size
import org.apache.spark.sql.functions.window
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL

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
        private val MASTER_URL = System.getenv("SPARK_MASTER_URL")
        private val CASSANDRA_HOST = System.getenv("APP_CASSANDRA_HOST")
        private val ELASTICSEARCH_HOST = System.getenv("APP_ELASTICSEARCH_HOST")
        private val ELASTICSEARCH_PORT = System.getenv("APP_ELASTICSEARCH_PORT")
        private val FP_MIN_SUPPORT = System.getenv("APP_FP_MIN_SUPPORT").toDouble()
        private val FP_MIN_CONFIDENCE = System.getenv("APP_FP_MIN_CONFIDENCE").toDouble()

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

            // Assign items to 1-minute time windows and collect them to a list
            // Example row: time-window1: [SignalName1, SignalName2, SignalName3]
            val dataFrame = sparkSession.createDataFrame(rdd, Item::class.java)
                .select("*")
                .groupBy(window(Column("timestamp"), "1 minute")) // TODO consider sliding windows
                .agg(collect_list("signalId"))
                .orderBy(col("window"))
                .withColumn("signals_set", array_distinct(col("collect_list(signalId)")))
                .drop("window", "collect_list(signalId)")

            // Perform FP-Mining via Spark MLlib's FPGrowth algorithm
            val model = FPGrowth()
                .setItemsCol("signals_set")
                .setMinSupport(FP_MIN_SUPPORT)
                .setMinConfidence(FP_MIN_CONFIDENCE)
                .fit(dataFrame)

            // Intermediary output of frequent item sets, can be removed
            model.freqItemsets().show(false)

            // Intermediary output of derived association rules, can be removed
            model.associationRules().show(false)

            // Get predictions on the original dataset.
            // Empty predictions are filtered out and duplicate ones removed.
            val predictions = model.transform(dataFrame)
                .filter(size(col("prediction")).`$greater`(0))
                .dropDuplicates()

            // Store the predictions in Elasticsearch for delivery, where operators can get notified about them
            JavaEsSparkSQL.saveToEs(predictions, "correlated-signal-predictions")
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
    }
}
