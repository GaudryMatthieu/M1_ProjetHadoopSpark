"""
PARTIE 1 (complément) - Ingestion batch d'un historique CSV dans le même Data Lake Parquet.
Utile pour avoir assez de volume pour entraîner le modèle (Partie 3).

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p1b_batch_ingest_csv.py
"""
from pyspark.sql import SparkSession
from common import LAKE_HIST_PATH, HDFS, read_lake, TX_SCHEMA, clean

spark = SparkSession.builder.appName("P1b-Batch-Ingest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Le CSV a été copié au préalable dans HDFS (voir README)
df = spark.read.csv(f"{HDFS}/raw/transactions.csv", header=True, schema=TX_SCHEMA)
tx = clean(df)
tx.write.mode("overwrite").partitionBy("date").parquet(LAKE_HIST_PATH)
print(f"{tx.count()} transactions ajoutées dans {LAKE_HIST_PATH}")
spark.read.parquet(LAKE_HIST_PATH).groupBy("type").count().show()
