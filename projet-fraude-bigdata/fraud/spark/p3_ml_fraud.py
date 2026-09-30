"""
PARTIE 3 - Modélisation IA avec Spark MLlib : prédiction de isFraud.
Comparaison Régression Logistique vs Random Forest ; AUC, matrice de confusion, rappel.
Les transactions prédites frauduleuses sont envoyées dans MongoDB (collection fraud_alerts)
pour l'agent IA de la Partie 4.

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --py-files /opt/spark/work-dir/common.py /opt/spark/work-dir/p3_ml_fraud.py
"""
import json
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from common import MONGO_URI, MONGO_DB, MODEL_PATH, read_lake

spark = SparkSession.builder.appName("P3-Fraud-MLlib").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = read_lake(spark)

# ======================= FEATURE ENGINEERING =======================
w_acc = Window.partitionBy("nameOrig")
w_acc_hour = Window.partitionBy("nameOrig", F.date_trunc("hour", "event_time"))
w_dest = Window.partitionBy("nameDest")

feat = (df
    .withColumn("log_amount", F.log1p("amount"))
    # Écart à la moyenne des transactions du compte (z-score)
    .withColumn("acc_mean", F.avg("amount").over(w_acc))
    .withColumn("acc_std", F.stddev("amount").over(w_acc))
    .withColumn("amount_zscore",
                F.when(F.col("acc_std") > 0, (F.col("amount") - F.col("acc_mean")) / F.col("acc_std"))
                 .otherwise(0.0))
    .withColumn("amount_ratio_mean", F.col("amount") / (F.col("acc_mean") + 1))
    # Fréquence horaire : nb de transactions du compte dans la même heure
    .withColumn("tx_same_hour", F.count("*").over(w_acc_hour))
    .withColumn("is_night", F.col("hour").isin(0, 1, 2, 3, 4, 5, 23).cast("int"))
    # Part du solde vidée par l'opération
    .withColumn("balance_drain_ratio",
                F.when(F.col("oldbalanceOrg") > 0, F.col("amount") / F.col("oldbalanceOrg")).otherwise(0.0))
    .withColumn("orig_emptied", (F.col("newbalanceOrig") <= 1).cast("int"))
    # Incohérences de soldes (signal classique sur PaySim)
    .withColumn("error_balance_orig", F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg"))
    .withColumn("error_balance_dest", F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest"))
    # Popularité du destinataire : nb d'expéditeurs distincts (mules / hubs)
    .withColumn("dest_nb_senders", F.size(F.collect_set("nameOrig").over(w_dest)))
    .withColumn("dest_is_merchant", F.col("nameDest").startswith("M").cast("int"))
    .fillna(0.0, subset=["amount_zscore", "acc_std"])
    .withColumn("label", F.col("isFraud").cast("double")))

NUM_FEATURES = ["log_amount", "amount_zscore", "amount_ratio_mean", "tx_same_hour", "hour", "is_night",
                "balance_drain_ratio", "orig_emptied", "error_balance_orig", "error_balance_dest",
                "dest_nb_senders", "dest_is_merchant", "oldbalanceOrg", "oldbalanceDest"]

# ======================= SPLIT STRATIFIÉ + POIDS DE CLASSE =======================
train = feat.sampleBy("label", fractions={0.0: 0.8, 1.0: 0.8}, seed=42)
test = feat.join(train.select("transaction_id"), "transaction_id", "left_anti")
train, test = train.cache(), test.cache()

ratio = train.filter("label = 1").count() / train.count()
print(f"Taux de fraude (train) : {ratio:.4f}")
# Rééquilibrage : la classe minoritaire pèse plus lourd
train = train.withColumn("weight", F.when(F.col("label") == 1, 1 - ratio).otherwise(ratio))

indexer = StringIndexer(inputCol="type", outputCol="type_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["type_idx"], outputCols=["type_ohe"])
assembler = VectorAssembler(inputCols=NUM_FEATURES + ["type_ohe"], outputCol="raw_features",
                            handleInvalid="skip")
scaler = StandardScaler(inputCol="raw_features", outputCol="features")

lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                        maxIter=50, regParam=0.01, elasticNetParam=0.0)
rf = RandomForestClassifier(featuresCol="features", labelCol="label", weightCol="weight",
                            numTrees=100, maxDepth=10, seed=42)

auc_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                         metricName="areaUnderROC")
pr_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                        metricName="areaUnderPR")


def evaluate(name, pred):
    cm = pred.groupBy("label", "prediction").count().collect()
    c = {(r["label"], r["prediction"]): r["count"] for r in cm}
    tp, fn = c.get((1.0, 1.0), 0), c.get((1.0, 0.0), 0)
    fp, tn = c.get((0.0, 1.0), 0), c.get((0.0, 0.0), 0)
    recall = tp / (tp + fn) if tp + fn else 0.0
    precision = tp / (tp + fp) if tp + fp else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    res = {"model": name, "AUC_ROC": round(auc_eval.evaluate(pred), 4),
           "AUC_PR": round(pr_eval.evaluate(pred), 4),
           "recall": round(recall, 4), "precision": round(precision, 4), "f1": round(f1, 4),
           "confusion_matrix": {"TN": tn, "FP": fp, "FN": fn, "TP": tp}}
    print(f"\n===== {name} =====")
    print(f"AUC ROC={res['AUC_ROC']}  AUC PR={res['AUC_PR']}  Rappel={res['recall']}  "
          f"Précision={res['precision']}  F1={res['f1']}")
    print("Matrice de confusion :")
    print(f"                 prédit 0   prédit 1")
    print(f"  réel 0 (légit) {tn:>9}  {fp:>9}")
    print(f"  réel 1 (fraude){fn:>9}  {tp:>9}")
    return res


results, models = [], {}
for name, clf in [("LogisticRegression", lr), ("RandomForest", rf)]:
    model = Pipeline(stages=[indexer, encoder, assembler, scaler, clf]).fit(train)
    pred = model.transform(test).cache()
    results.append(evaluate(name, pred))
    models[name] = (model, pred)

# Importance des variables (Random Forest)
rf_model = models["RandomForest"][0].stages[-1]
type_labels = models["RandomForest"][0].stages[0].labels
feat_names = NUM_FEATURES + [f"type_{t}" for t in type_labels]
importances = sorted(zip(feat_names, rf_model.featureImportances.toArray()), key=lambda x: -x[1])
print("\nImportance des variables (RF) :")
for n, v in importances[:10]:
    print(f"  {n:<22} {v:.4f}")

# Coefficients de la régression logistique (variables standardisées)
lr_model = models["LogisticRegression"][0].stages[-1]
coefs = sorted(zip(feat_names, lr_model.coefficients.toArray()), key=lambda x: -abs(x[1]))

with open("/opt/spark/work-dir/p3_metrics.json", "w") as f:
    json.dump({"results": results,
               "rf_feature_importance": [(n, round(float(v), 4)) for n, v in importances],
               "lr_coefficients": [(n, round(float(v), 4)) for n, v in coefs]}, f, indent=2)
print("Métriques sauvegardées dans spark/p3_metrics.json")

# Choix du meilleur modèle selon le rappel puis l'AUC PR
best = max(results, key=lambda r: (r["recall"], r["AUC_PR"]))["model"]
best_model, best_pred = models[best]
best_model.write().overwrite().save(MODEL_PATH)
print(f"Meilleur modèle : {best} -> sauvegardé dans {MODEL_PATH}")

# ======================= ALERTES POUR L'AGENT IA (Partie 4) =======================
alerts = (best_pred.filter("prediction = 1")
          .withColumn("fraud_score", F.round(vector_to_array("probability")[1], 4))
          .select("transaction_id", F.col("event_time").cast("string").alias("event_time"),
                  "type", "amount", "nameOrig", "nameDest", "oldbalanceOrg", "newbalanceOrig",
                  "hour", "is_night", "amount_zscore", "acc_mean", "balance_drain_ratio",
                  "orig_emptied", "tx_same_hour", "dest_nb_senders", "isFraud", "fraud_score")
          .withColumn("_id", F.col("transaction_id"))
          .withColumn("model", F.lit(best))
          .withColumn("status", F.lit("NEW")))

(alerts.write.format("mongodb").mode("overwrite")
   .option("connection.uri", MONGO_URI).option("database", MONGO_DB)
   .option("collection", "fraud_alerts").save())
print(f"{alerts.count()} alertes écrites dans {MONGO_DB}.fraud_alerts")
