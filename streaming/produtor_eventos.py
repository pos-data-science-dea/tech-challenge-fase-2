"""Produtor simulador de eventos de alfabetização (ingestão streaming).

Publica eventos de "nova medição de indicador" num tópico Pub/Sub, simulando
ingestão em tempo quase real. Os eventos são consumidos por uma BigQuery
Subscription e pousam na bronze de streaming.

Uso:
    pip install google-cloud-pubsub
    python produtor_eventos.py --qtd 30 --intervalo 2
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

PROJECT = "tech-challenge-fase-2-505123"
TOPIC = "eventos-alfabetizacao"

# amostra de códigos IBGE reais, para os eventos casarem com a dim_municipio
MUNICIPIOS = ["3550308", "3304557", "2927408", "5300108",
              "2304400", "4106902", "1302603", "2611606"]
REDES = ["Federal", "Municipal", "Estadual"]


def gerar_evento() -> dict:
    return {
        "evento_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "tipo_evento": "nova_medicao_indicador",
        "ano": 2025,
        "id_municipio": random.choice(MUNICIPIOS),
        "rede": random.choice(REDES),
        "taxa_alfabetizacao": round(random.uniform(40, 99), 2),
        "media_portugues": round(random.uniform(700, 850), 2),
        "fonte": "simulador_streaming",
    }


def main(qtd: int, intervalo: float) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)
    print(f"Publicando {qtd} eventos em {topic_path} (intervalo {intervalo}s)")
    for i in range(qtd):
        evento = gerar_evento()
        data = json.dumps(evento).encode("utf-8")
        future = publisher.publish(topic_path, data, tipo=evento["tipo_evento"])
        print(f"[{i+1}/{qtd}] {evento['id_municipio']}/{evento['rede']} "
              f"taxa={evento['taxa_alfabetizacao']} -> msg {future.result()}")
        time.sleep(intervalo)
    print("Concluído.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--qtd", type=int, default=30, help="quantidade de eventos")
    ap.add_argument("--intervalo", type=float, default=2.0, help="segundos entre eventos")
    main(**vars(ap.parse_args()))
