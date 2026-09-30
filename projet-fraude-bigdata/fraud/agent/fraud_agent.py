"""
PARTIE 4 - Agent IA : explique les alertes de fraude (conseiller) et rédige l'email client.

Pour chaque alerte "NEW" de MongoDB (produite par la Partie 3) l'agent :
  1. récupère les métadonnées de la transaction et calcule les motifs du score de risque ;
  2. génère une explication vulgarisée pour le conseiller bancaire ;
  3. rédige un email personnalisé et rassurant demandant au client de valider l'opération dans l'app.

LLM : Ollama local (Llama 3) par défaut, ou GPT-4o si OPENAI_API_KEY est défini.
    docker exec -it ollama ollama pull llama3
    python fraud_agent.py --limit 5
"""
import argparse
import json
import os
from datetime import datetime

import requests
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

SYSTEM_PROMPT = """Tu es un assistant de la cellule anti-fraude d'une banque française.
Tu reçois une transaction signalée par un modèle de Machine Learning et les motifs du score.
Règles : n'invente aucun fait absent des données ; ne demande JAMAIS au client un mot de passe,
un code, ni de cliquer sur un lien ; invite-le à valider ou refuser l'opération uniquement depuis
son application bancaire officielle. Réponds STRICTEMENT en JSON avec deux clés :
"explication_conseiller" (5 phrases max, vulgarisée, cite les motifs et le score) et
"email_client" (objet + corps, ton rassurant et sécuritaire, vouvoiement, signé "Le service Sécurité")."""


def risk_reasons(a):
    """Traduit les variables du modèle en motifs lisibles (pour le prompt et l'audit)."""
    r = []
    if a.get("balance_drain_ratio", 0) >= 0.8 or a.get("orig_emptied"):
        r.append(f"l'opération vide {min(a.get('balance_drain_ratio', 0), 1):.0%} du solde du compte")
    if a.get("amount_zscore", 0) >= 3:
        r.append(f"montant {a['amount']:.0f} € très supérieur aux habitudes du client "
                 f"(moyenne {a.get('acc_mean', 0):.0f} €)")
    if a.get("is_night"):
        r.append(f"opération réalisée en pleine nuit ({a.get('hour')}h)")
    if a.get("dest_nb_senders", 0) >= 10:
        r.append(f"le bénéficiaire reçoit des fonds de {a['dest_nb_senders']} comptes différents "
                 f"(profil de compte collecteur)")
    if a.get("tx_same_hour", 0) >= 3:
        r.append(f"{a['tx_same_hour']} opérations du même compte dans la même heure")
    if a.get("type") in ("TRANSFER", "CASH_OUT"):
        r.append(f"type d'opération à risque ({a['type']})")
    return r or ["combinaison inhabituelle de caractéristiques détectée par le modèle"]


def call_llm(user_prompt):
    if OPENAI_KEY:
        resp = requests.post("https://api.openai.com/v1/chat/completions",
                             headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                             json={"model": "gpt-4o", "temperature": 0.3,
                                   "response_format": {"type": "json_object"},
                                   "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                                                {"role": "user", "content": user_prompt}]},
                             timeout=120)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    resp = requests.post(f"{OLLAMA_URL}/api/chat",
                         json={"model": OLLAMA_MODEL, "stream": False, "format": "json",
                               "options": {"temperature": 0.3},
                               "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                                            {"role": "user", "content": user_prompt}]},
                         timeout=300)
    resp.raise_for_status()
    return resp.json()["message"]["content"]


def process(alert):
    reasons = risk_reasons(alert)
    meta = {k: alert.get(k) for k in ("transaction_id", "event_time", "type", "amount",
                                      "nameOrig", "nameDest", "oldbalanceOrg", "newbalanceOrig")}
    prompt = (f"Transaction signalée :\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n"
              f"Score de risque du modèle ({alert.get('model')}) : {alert.get('fraud_score')}\n"
              f"Motifs : {'; '.join(reasons)}\n"
              f"Client : titulaire du compte {alert.get('nameOrig')}.")
    raw = call_llm(prompt)
    try:
        out = json.loads(raw)
    except json.JSONDecodeError:
        out = {"explication_conseiller": raw, "email_client": ""}
    return reasons, out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=5)
    a = p.parse_args()

    db = MongoClient(MONGO_URI)["fraud_lab"]
    alerts = list(db.fraud_alerts.find({"status": "NEW"}).sort("fraud_score", -1).limit(a.limit))
    print(f"{len(alerts)} alertes à traiter")
    for al in alerts:
        reasons, out = process(al)
        doc = {"_id": al["_id"], "transaction_id": al["transaction_id"],
               "fraud_score": al.get("fraud_score"), "reasons": reasons,
               "explication_conseiller": out.get("explication_conseiller"),
               "email_client": out.get("email_client"),
               "llm": "gpt-4o" if OPENAI_KEY else OLLAMA_MODEL,
               "generated_at": datetime.utcnow().isoformat()}
        db.fraud_explanations.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        db.fraud_alerts.update_one({"_id": al["_id"]}, {"$set": {"status": "EXPLAINED"}})
        print("=" * 80)
        print(f"Transaction {al['transaction_id']} | score {al.get('fraud_score')} | "
              f"vraie fraude : {al.get('isFraud')}")
        print("--- Pour le conseiller ---\n", doc["explication_conseiller"])
        print("--- Email client ---\n", doc["email_client"])


if __name__ == "__main__":
    main()
