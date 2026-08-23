import argparse
import asyncio
import json
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from kafka import KafkaProducer

from faker import Faker

fake = Faker()

CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports", "toys", "books"]
DEVICES = ["mobile", "desktop", "tablet"]
REFERRERS = ["google", "direct", "email", "social", "affiliate"]


def build_catalog(n_products: int=200):
    catalog = []

    for i in range(n_products):
        catalog.append({
            "product_id": f"P{i:05d}",
            "name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(5,300),2)
        })
    return catalog

def build_user(n_users: int=200):
    return  [f"U{i:06d}" for i in range(n_users)]


TRANSITIONS = {
    "start": [("browse", 1.0)],
    "browse": [("view_product", 0.75), ("exit", 0.25)],
    "view_product": [("view_product", 0.30),("add_to_cart", 0.30) ("exit", 0.40)],
    "add_to_cart": [("view_product", 0.25),("checkout", 0.35) ("exit", 0.40)],
    "checkout": [("purchase", 0.75), ("exit", 0.25)],
    "purchase": [("exit", 1.0)]
}

def next_state(state: str) -> str:
    options, weights = zip(*TRANSITIONS[state])

    return random.choice(options, weights=weights, k=1)[0]

class Sink:
    async def send(self, event: dict):
        raise NotImplementedError
    async def close(self):
        pass

class ConsoleSink(Sink):
    async def send(self, event: dict):
        print(json.dumps(event))

class FileSink(Sink):
    def __init__(self, path:str):
        self.f = open(path, "a")

    async def send(self, event: dict):
        self.f.write(json.dumps(event)+ "\n")
        self.f.flush()

    async def close(self):
        self.f.close()

class KafkaSink(Sink):
    def __init__(self, bootstrap: str, topic: str, security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str = None, sasl_username: str = None, sasl_password: str = None):
        
        self.topic = topic

        kwargs = dict(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            security_protocol=security_protocol,
        )

        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            kwargs.update(
                sasl_mechanism=sasl_mechanism or "PLAIN",
                sasl_plain_username=sasl_username,
                sasl_plain_password=sasl_password,
            )

        self.producer = KafkaProducer(**kwargs)

    async def send(self, event: dict):
        self.producer.send(self.topic, key=event.get("session_id"), value=event)

    async def close(self):
        self.producer.flush()
        self.producer.close()