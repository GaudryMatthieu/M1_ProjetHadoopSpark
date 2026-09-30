# Projet Big Data – Détection de fraude (Kafka · HDFS · Spark · MongoDB · Gephi · LLM)

## Architecture
```
generate_transactions.py ──JSON──> Kafka (topic transactions)
                                      │
                         Spark Structured Streaming (P1)
                     ┌────────────────┴─────────────────┐
           HDFS Parquet (Data Lake)          MongoDB metrics_per_minute
                     │
     Spark batch (P2) ──> MongoDB graph_nodes / graph_edges ──mongoexport──> Gephi
     Spark MLlib (P3) ──> MongoDB fraud_alerts ──> Agent LLM (P4) ──> fraud_explanations
```

## Dataset
Le sujet recommande PaySim (`isFraud`). Les transactions sont générées par
`producer/generate_transactions.py` au format PaySim et **calibrées sur BankChurners.csv** :
chaque CLIENTNUM devient un compte, son montant moyen = Total_Trans_Amt / Total_Trans_Ct, son solde
initial ≈ Avg_Open_To_Buy. Trois scénarios de fraude sont injectés (prise de contrôle + cash-out,
anneau de blanchiment entre 4 hubs, smurfing) pour obtenir une cible `isFraud` et un réseau visible dans Gephi.

## Lancement pas à pas
```bash
# 0. Démarrer le cluster
docker compose up -d --build
pip install -r producer/requirements.txt
docker exec -it kafka kafka-topics --create --topic transactions --partitions 3 \
  --replication-factor 1 --bootstrap-server localhost:9092

# 1a. Historique (volume pour le ML) : CSV -> HDFS -> Parquet
cd producer && python generate_transactions.py --mode csv --n 300000 && cd ..
docker exec -it namenode hdfs dfs -mkdir -p /raw
docker cp data/transactions.csv namenode:/tmp/transactions.csv
docker exec -it namenode hdfs dfs -put -f /tmp/transactions.csv /raw/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p1b_batch_ingest_csv.py

# 1b. Streaming (2 terminaux)
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p1_streaming_kafka_hdfs.py
cd producer && python generate_transactions.py --mode kafka --rate 50 --live-time

# 2. Graphe -> MongoDB -> Gephi
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p2_hdfs_to_mongo_graph.py
bash gephi/export_for_gephi.sh

# 3. Machine Learning
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p3_ml_fraud.py

# 4. Agent IA
docker exec -it ollama ollama pull llama3
cd agent && python fraud_agent.py --limit 5
```

## Interfaces
| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| HDFS | http://localhost:9870 |
| Kafdrop (topics Kafka) | http://localhost:9000 |
| Mongo Express | http://localhost:8082 |

## Gephi
1. Fichier > Importer feuille de calcul : `nodes_suspect.csv` (table des nœuds) puis `edges_suspect.csv` (arêtes, « ajouter à l'espace de travail existant »).
2. Statistiques : **Degré moyen** et **Modularité** (+ Centralité d'intermédiarité).
3. Apparence : taille des nœuds = Degré entrant, couleur = classe de modularité (ou `fraud_involved`).
4. Spatialisation : ForceAtlas 2. Les hubs de blanchiment apparaissent comme de gros nœuds reliés en anneau.

## Points à mettre dans le rapport
- **Parquet** : stockage colonne compressé, lecture sélective des colonnes, partitionnement par date.
- **Checkpoints** HDFS : reprise exacte du streaming après panne ; watermark de 2 min pour les retards.
- **Deux zones du lake** (stream/hist) : un dossier écrit par un sink streaming a un journal `_spark_metadata`.
- **Déséquilibre de classes** : pondération des classes ; on privilégie le **rappel** (une fraude ratée coûte plus qu'une fausse alerte) et l'AUC PR.
- **Limite** : `dest_nb_senders` et la moyenne du compte sont calculées sur tout l'historique ; en production il faut les calculer sur une fenêtre passée uniquement (risque de fuite d'information).
