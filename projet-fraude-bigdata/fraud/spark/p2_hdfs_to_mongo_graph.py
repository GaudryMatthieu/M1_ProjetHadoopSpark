"""
PARTIE 2 - Pipeline HDFS -> Data Lab MongoDB, modélisé en graphe (Nodes = comptes, Edges = flux).

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p2_hdfs_to_mongo_graph.py

Choix : on ne garde que les flux compte -> compte susceptibles de déplacer de l'argent
(TRANSFER, CASH_OUT). Les PAYMENT vers marchands créeraient d'énormes "étoiles" sans intérêt
pour l'analyse de blanchiment et rendraient le graphe illisible dans Gephi.
Les colonnes sont nommées Id / Label / Source / Target / Weight pour un import direct dans Gephi.
"""
from pyspark.sql import SparkSession, functions as F
from common import MONGO_URI, MONGO_DB, read_lake

spark = SparkSession.builder.appName("P2-HDFS-to-Mongo-Graph").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def to_mongo(df, collection):
    (df.write.format("mongodb").mode("overwrite")
       .option("connection.uri", MONGO_URI)
       .option("database", MONGO_DB)
       .option("collection", collection)
       .save())
    print(f"-> {MONGO_DB}.{collection} : {df.count()} documents")


tx = read_lake(spark).filter(F.col("type").isin("TRANSFER", "CASH_OUT")).cache()

# ---------- EDGES : agrégation des transactions entre deux comptes ----------
edges = (tx.groupBy(F.col("nameOrig").alias("Source"), F.col("nameDest").alias("Target"))
         .agg(F.count("*").alias("Weight"),                       # nb de transactions = poids
              F.round(F.sum("amount"), 2).alias("total_amount"),
              F.round(F.avg("amount"), 2).alias("avg_amount"),
              F.sum("isFraud").alias("fraud_count"),
              F.min("event_time").cast("string").alias("first_seen"),
              F.max("event_time").cast("string").alias("last_seen"))
         .withColumn("Type", F.lit("Directed"))
         .withColumn("suspicious", (F.col("fraud_count") > 0).cast("int")))

# ---------- NODES : un document par compte ----------
out_stats = (tx.groupBy(F.col("nameOrig").alias("Id"))
             .agg(F.count("*").alias("out_tx"),
                  F.countDistinct("nameDest").alias("out_partners"),
                  F.round(F.sum("amount"), 2).alias("total_sent"),
                  F.sum("isFraud").alias("fraud_sent")))
in_stats = (tx.groupBy(F.col("nameDest").alias("Id"))
            .agg(F.count("*").alias("in_tx"),
                 F.countDistinct("nameOrig").alias("in_partners"),
                 F.round(F.sum("amount"), 2).alias("total_received"),
                 F.sum("isFraud").alias("fraud_received")))

nodes = (out_stats.join(in_stats, "Id", "full_outer")
         .fillna(0)
         .withColumn("Label", F.col("Id"))
         .withColumn("account_type", F.when(F.col("Id").startswith("M"), "merchant").otherwise("customer"))
         .withColumn("fraud_involved", ((F.col("fraud_sent") + F.col("fraud_received")) > 0).cast("int"))
         # ratio reçu/envoyé : un compte "mule" reçoit et ressort presque tout
         .withColumn("pass_through_ratio",
                     F.when(F.col("total_received") > 0,
                            F.round(F.col("total_sent") / F.col("total_received"), 3)).otherwise(0.0)))

# Sous-graphe "suspect" : arêtes frauduleuses + toutes les arêtes des comptes impliqués
# (plus lisible dans Gephi que le graphe complet)
suspects = nodes.filter("fraud_involved = 1").select("Id")
edges_susp = (edges.join(suspects.withColumnRenamed("Id", "Source"), "Source", "left_semi")
              .unionByName(edges.join(suspects.withColumnRenamed("Id", "Target"), "Target", "left_semi"))
              .dropDuplicates(["Source", "Target"]))
ids_susp = (edges_susp.select(F.col("Source").alias("Id"))
            .union(edges_susp.select(F.col("Target").alias("Id"))).distinct())
nodes_susp = nodes.join(ids_susp, "Id", "left_semi")

to_mongo(nodes, "graph_nodes")
to_mongo(edges, "graph_edges")
to_mongo(nodes_susp, "graph_nodes_suspect")
to_mongo(edges_susp, "graph_edges_suspect")

print("Top 10 comptes recevant de plus d'expéditeurs distincts (candidats 'hubs') :")
nodes.orderBy(F.desc("in_partners")).select("Id", "in_partners", "total_received",
                                            "pass_through_ratio", "fraud_involved").show(10, False)
