"""
Générateur de transactions financières au format PaySim, calibré sur BankChurners.csv.

- Chaque client de BankChurners (CLIENTNUM) devient un compte "C<CLIENTNUM>".
- Le montant moyen d'un client = Total_Trans_Amt / Total_Trans_Ct, le solde initial = Avg_Open_To_Buy.
- Des scénarios de fraude réalistes sont injectés (cible isFraud) :
    1. Prise de contrôle de compte : TRANSFER de quasi tout le solde vers un compte "mule",
       puis CASH_OUT immédiat de la mule.
    2. Réseau de blanchiment : quelques comptes mules "hubs" reçoivent depuis de nombreuses victimes
       et se transfèrent l'argent entre eux (anneau) -> visible dans Gephi.
    3. Smurfing : plusieurs petits virements rapides vers la même mule.

Modes :
    python generate_transactions.py --mode csv   --n 300000 --out ../data/transactions.csv
    python generate_transactions.py --mode kafka --rate 50  --topic transactions
"""
import argparse
import csv
import json
import math
import random
import time
from datetime import datetime, timedelta

FIELDS = ["transaction_id", "step", "timestamp", "type", "amount",
          "nameOrig", "oldbalanceOrg", "newbalanceOrig",
          "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"]

TYPES_NORMAL = ["PAYMENT", "PAYMENT", "PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]


class Simulator:
    def __init__(self, churn_csv, fraud_rate=0.012, seed=42, n_mules=40, n_merchants=2000):
        random.seed(seed)
        self.fraud_rate = fraud_rate
        self.clients = {}
        with open(churn_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cid = "C" + row["CLIENTNUM"]
                ct = max(float(row["Total_Trans_Ct"]), 1.0)
                self.clients[cid] = {
                    "avg": float(row["Total_Trans_Amt"]) / ct,
                    "balance": float(row["Avg_Open_To_Buy"]) + float(row["Credit_Limit"]) * 0.2,
                    "churn": row["Attrition_Flag"] != "Existing Customer",
                }
        self.client_ids = list(self.clients.keys())
        self.merchants = [f"M{random.randint(10**8, 10**9 - 1)}" for _ in range(n_merchants)]
        # Comptes mules (n'appartiennent pas à BankChurners)
        self.mules = [f"C9{random.randint(10**7, 10**8 - 1)}" for _ in range(n_mules)]
        for m in self.mules:
            self.clients[m] = {"avg": 200.0, "balance": 500.0, "churn": False}
        # 4 "hubs" de blanchiment qui centralisent les fonds
        self.hubs = self.mules[:4]
        self.balances_dest = {}
        self.step = 0
        self.t0 = datetime(2026, 1, 1)
        self.counter = 0

    # ---------- utilitaires ----------
    def _ts(self):
        # 1 step = 1 heure comme dans PaySim ; activité plus faible la nuit
        return self.t0 + timedelta(hours=self.step, minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    def _tx(self, ttype, amount, orig, dest, is_fraud, ts=None):
        self.counter += 1
        amount = round(max(amount, 1.0), 2)
        o = self.clients.get(orig)
        old_o = round(o["balance"], 2) if o else 0.0
        if ttype == "CASH_IN":
            new_o = old_o + amount
        else:
            new_o = max(old_o - amount, 0.0)
        if o:
            o["balance"] = new_o
        d = self.clients.get(dest)
        if d is not None:
            old_d = round(d["balance"], 2)
            new_d = old_d + amount if ttype in ("TRANSFER", "CASH_IN") else old_d
            if ttype in ("TRANSFER",):
                d["balance"] = new_d
        else:  # marchand : PaySim met les soldes marchands à 0
            old_d, new_d = 0.0, 0.0
        ts = ts or self._ts()
        return {
            "transaction_id": f"T{self.counter:010d}",
            "step": self.step,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "type": ttype,
            "amount": amount,
            "nameOrig": orig,
            "oldbalanceOrg": round(old_o, 2),
            "newbalanceOrig": round(new_o, 2),
            "nameDest": dest,
            "oldbalanceDest": round(old_d, 2),
            "newbalanceDest": round(new_d, 2),
            "isFraud": int(is_fraud),
        }

    # ---------- comportements ----------
    def normal(self):
        orig = random.choice(self.client_ids)
        c = self.clients[orig]
        ttype = random.choice(TYPES_NORMAL)
        amount = random.lognormvariate(math.log(max(c["avg"], 5)), 0.6)
        if ttype in ("PAYMENT", "DEBIT"):
            dest = random.choice(self.merchants)
        else:
            dest = random.choice(self.client_ids)
        if ttype == "CASH_IN":
            c["balance"] += 0  # géré dans _tx
        return [self._tx(ttype, amount, orig, dest, False)]

    def account_takeover(self):
        victim = random.choice(self.client_ids)
        mule = random.choice(self.hubs) if random.random() < 0.6 else random.choice(self.mules)
        bal = self.clients[victim]["balance"]
        amount = bal * random.uniform(0.9, 1.0) if bal > 50 else random.uniform(2000, 9000)
        # fraude plutôt la nuit
        ts = self.t0 + timedelta(hours=self.step, minutes=random.randint(0, 59))
        ts = ts.replace(hour=random.choice([0, 1, 2, 3, 4, 23]))
        txs = [self._tx("TRANSFER", amount, victim, mule, True, ts)]
        txs.append(self._tx("CASH_OUT", amount * 0.97, mule, random.choice(self.merchants), True,
                            ts + timedelta(minutes=random.randint(1, 15))))
        return txs

    def laundering_ring(self):
        # les hubs se renvoient les fonds en anneau
        txs = []
        amount = random.uniform(5000, 50000)
        ring = random.sample(self.hubs, len(self.hubs))
        ts = self._ts()
        for i in range(len(ring)):
            txs.append(self._tx("TRANSFER", amount * random.uniform(0.95, 1.0), ring[i],
                                ring[(i + 1) % len(ring)], True, ts + timedelta(minutes=5 * i)))
        return txs

    def smurfing(self):
        victim = random.choice(self.client_ids)
        mule = random.choice(self.mules)
        ts = self._ts()
        return [self._tx("TRANSFER", random.uniform(500, 999), victim, mule, True,
                         ts + timedelta(minutes=2 * i)) for i in range(random.randint(3, 6))]

    def next_batch(self):
        """Renvoie une liste de transactions (1 ou plusieurs si scénario de fraude)."""
        if random.random() < 0.01:
            self.step += 1
        r = random.random()
        if r < self.fraud_rate * 0.6:
            return self.account_takeover()
        if r < self.fraud_rate * 0.8:
            return self.smurfing()
        if r < self.fraud_rate * 0.85:
            return self.laundering_ring()
        return self.normal()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--churn", default="../data/BankChurners.csv")
    p.add_argument("--mode", choices=["csv", "kafka"], default="csv")
    p.add_argument("--n", type=int, default=300000, help="nb de transactions (mode csv)")
    p.add_argument("--out", default="../data/transactions.csv")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--rate", type=float, default=50, help="transactions / seconde (mode kafka)")
    p.add_argument("--live-time", action="store_true",
                   help="mode kafka : horodatage = heure actuelle (métriques temps réel lisibles)")
    a = p.parse_args()

    sim = Simulator(a.churn)

    if a.mode == "csv":
        n = 0
        with open(a.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            while n < a.n:
                for tx in sim.next_batch():
                    w.writerow(tx)
                    n += 1
        print(f"{n} transactions écrites dans {a.out}")
        return

    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=a.bootstrap,
                             key_serializer=lambda k: k.encode("utf-8"),
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    print(f"Envoi vers Kafka {a.bootstrap} topic={a.topic} à ~{a.rate} tx/s (Ctrl+C pour arrêter)")
    sent = 0
    try:
        while True:
            for tx in sim.next_batch():
                if a.live_time:
                    tx["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                producer.send(a.topic, key=tx["nameOrig"], value=tx)
                sent += 1
                if sent % 500 == 0:
                    print(f"{sent} transactions envoyées")
                time.sleep(1.0 / a.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"Total envoyé : {sent}")


if __name__ == "__main__":
    main()
