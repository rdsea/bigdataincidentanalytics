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
        @JvmStatic fun main(args: Array<String>) {
            val conf = SparkConf(true)
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.sql.session.timeZone", "UTC")
                .set("spark.es.nodes","192.168.0.95")
                .set("spark.es.port","9200")
                .set("spark.es.index.auto.create", "true")
            val sc = SparkContext("spark://MacBook-Pro.local:7077", "Frequent Pattern Mining", conf)
            sc.setLogLevel("WARN")
            val spark = SparkSession.builder()
                .sparkContext(sc)
                .orCreate
            val rdd: JavaRDD<Item> = javaFunctions(sc)
                .cassandraTable("incident_analytics", "signals", mapRowTo(SignalRecord::class.java))
                .select(
                    column("uuid"),
                    column("signal_name").`as`("name"),
                    column("pipeline_component").`as`("pipelineComponent"),
                    column("timestamp")
                ).map { Item("${it.name}#${it.pipelineComponent}", it.timestamp) }
            val df = spark.createDataFrame(rdd, Item::class.java)

            val ds = df.select("*")
                .groupBy(window(Column("timestamp"), "1 minute")) // TODO consider sliding windows
                .agg(collect_list("signalId"))
                .orderBy(col("window"))
                .withColumn("signals_set", array_distinct(col("collect_list(signalId)")))
                .drop("window", "collect_list(signalId)")

            val model = FPGrowth()
                .setItemsCol("signals_set")
                .setMinSupport(0.1)
                .setMinConfidence(0.5)
                .fit(ds)

            println("Frequent item sets:\n")
            model.freqItemsets().show(false)
            /* Example output
            +---------------------------------------------------------+----+
            |items                                                    |freq|
            +---------------------------------------------------------+----+
            |[MqttStartupLog#MQTT_BROKER]                             |9   |
            |[MqttStartupLog#MQTT_BROKER, MqttShutdownLog#MQTT_BROKER]|8   |
            +---------------------------------------------------------+----+

             */

            println("Association rules:\n")
            model.associationRules().show(false)
            /* Example output
            +-----------------------------+-----------------------------+------------------+------------------+
            |antecedent                   |consequent                   |confidence        |lift              |
            +-----------------------------+-----------------------------+------------------+------------------+
            |[MqttShutdownLog#MQTT_BROKER]|[MqttStartupLog#MQTT_BROKER] |0.7272727272727273|1.0505050505050506|
            +-----------------------------+-----------------------------+------------------+------------------+

             */

            println("Predictions:\n")
            val predictions = model.transform(ds)
            predictions.show(false)
            val x = predictions.filter(size(col("prediction")).`$greater`(0))
                .dropDuplicates()
            JavaEsSparkSQL.saveToEs(x, "correlated-signal-predictions")

            /* ds.select("*").orderBy(col("window")).show(20, false)
            ds.withColumn("window", ds.col("window").cast(DataTypes.StringType))
                .withColumn("unique_signals", ds.col("unique_signals").cast(DataTypes.StringType))
                .drop("collect_list(signalId)")
                .coalesce(1)
                .write()
                .option("header", true)
                .csv("/Users/danielfuvesi/Repositories/bigdataincidentanalytics/reasoning/pattern-analysis/table.csv")*/

            /*val df = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .options(mapOf("table" to "signals","keyspace" to "incident_analytics"))
                .schema(StructType()
                    .add(StructField("uuid",DataTypes.StringType, false, Metadata.empty()))
                    .add(StructField("pipeline_component",DataTypes.StringType, false, Metadata.empty()))
                    .add(StructField("signal_name",DataTypes.StringType, false, Metadata.empty()))
                    .add(StructField("timestamp",DataTypes.TimestampType, false, Metadata.empty()))
                )
                .load()
            df.select()*/
        }
    }
}
