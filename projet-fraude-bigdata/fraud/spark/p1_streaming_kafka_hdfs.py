"""
PARTIE 1 - Streaming : Kafka -> Spark Structured Streaming -> HDFS (Parquet) + métriques temps réel.

Lancement :
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --py-files /opt/spark/work-dir/common.py \
  /opt/spark/work-dir/p1_streaming_kafka_hdfs.py
"""
from pyspark.sql import SparkSession, functions as F
from common import (KAFKA_BOOTSTRAP, KAFKA_TOPIC, LAKE_PATH, CHECKPOINTS,
                    MONGO_URI, MONGO_DB, TX_SCHEMA, clean)

spark = (SparkSession.builder.appName("P1-Kafka-to-HDFS")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Lecture du topic Kafka (key/value binaires)
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", KAFKA_TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

# 2. Cast en String puis parsing JSON avec un schéma explicite
parsed = (raw.select(F.col("value").cast("string").alias("json"),
                     F.col("timestamp").alias("kafka_ts"))
          .select(F.from_json("json", TX_SCHEMA).alias("tx"), "kafka_ts")
          .select("tx.*", "kafka_ts"))

# 3. Transformation à la volée (typage, nettoyage, colonnes dérivées)
tx = clean(parsed).withWatermark("event_time", "2 minutes")

# 4a. Persistance dans le Data Lake HDFS en Parquet, partitionné par date
q_lake = (tx.drop("kafka_ts").writeStream
          .format("parquet")
          .option("path", LAKE_PATH)
          .option("checkpointLocation", f"{CHECKPOINTS}/lake")
          .partitionBy("date")
          .outputMode("append")
          .trigger(processingTime="30 seconds")
          .start())

# 4b. Métriques temps réel : volume / minute et montant total par type
metrics = (tx.groupBy(F.window("event_time", "1 minute"), "type")
           .agg(F.count("*").alias("nb_transactions"),
                F.round(F.sum("amount"), 2).alias("montant_total"),
                F.round(F.avg("amount"), 2).alias("montant_moyen"),
                F.sum("isFraud").alias("nb_fraudes"))
           .select(F.col("window.start").alias("minute_debut"),
                   F.col("window.end").alias("minute_fin"),
                   "type", "nb_transactions", "montant_total", "montant_moyen", "nb_fraudes"))


def write_metrics(batch_df, batch_id):
    """Upsert des métriques (mode update) dans MongoDB + affichage console."""
    if batch_df.rdd.isEmpty():
        return
    out = batch_df.withColumn("_id", F.concat_ws("|", F.col("minute_debut").cast("string"), "type"))
    out.orderBy(F.desc("minute_debut")).show(10, truncate=False)
    (out.write.format("mongodb").mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", "metrics_per_minute")
        .option("operationType", "replace")
        .save())


q_metrics = (metrics.writeStream
             .outputMode("update")
             .foreachBatch(write_metrics)
             .option("checkpointLocation", f"{CHECKPOINTS}/metrics")
             .trigger(processingTime="20 seconds")
             .start())

spark.streams.awaitAnyTermination()
