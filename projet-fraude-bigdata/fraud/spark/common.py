"""Paramètres partagés par tous les scripts Spark."""
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, IntegerType)

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "transactions"
HDFS = "hdfs://namenode:9000"
LAKE_PATH = f"{HDFS}/datalake/transactions_stream"   # Parquet écrit par le streaming
LAKE_HIST_PATH = f"{HDFS}/datalake/transactions_hist"  # Parquet écrit par l'ingestion batch
CHECKPOINTS = f"{HDFS}/checkpoints"
MODEL_PATH = f"{HDFS}/models/fraud_rf"
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "fraud_lab"

TX_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("step", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType()),
])

VALID_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]


def clean(df):
    """Typage + nettoyage commun au streaming et au batch."""
    from pyspark.sql import functions as F
    return (df
            .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("type", F.upper(F.trim("type")))
            .filter(F.col("transaction_id").isNotNull())
            .filter(F.col("event_time").isNotNull())
            .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
            .filter(F.col("type").isin(VALID_TYPES))
            .fillna(0.0, subset=["oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"])
            .fillna(0, subset=["isFraud"])
            .withColumn("hour", F.hour("event_time"))
            .withColumn("date", F.to_date("event_time"))
            .drop("timestamp"))


def read_lake(spark):
    """Lit le Data Lake (streaming + historique batch).
    Les deux zones sont séparées car un dossier écrit par un sink streaming possède un
    journal _spark_metadata : des fichiers ajoutés en batch dans ce dossier seraient ignorés."""
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(HDFS), spark._jsc.hadoopConfiguration())
    dfs = [spark.read.parquet(p) for p in (LAKE_PATH, LAKE_HIST_PATH)
           if fs.exists(jvm.org.apache.hadoop.fs.Path(p))]
    if not dfs:
        raise RuntimeError("Data Lake vide : lancez d'abord la Partie 1")
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out.dropDuplicates(["transaction_id"])
