import argparse
import asyncio
import json
import random
import sys
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

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

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
        from kafka import KafkaProducer
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

class WebhookSink(Sink):
    def __init__(self, url:str):
        import requests
        self.requests = requests
        self.url = url
        self.session = requests.Session()
    async def send(self, event: dict):
        self.session.post(self.url, json=event, timeout=5)


def make_sink(args) -> Sink:
    if args.sink == "console":
        return ConsoleSink()

    if args.sink == "file":
        if not args.output:
            sys.exit(" --ouput required for --sink file.")
        return FileSink(args.output)

    if args.sink == "kafka":
        if not args.kafka_topic:
            sys.exit("--kafka-topic required for --sink kafka.")
        return KafkaSink(
            args.kafka_bootstrap,
            args.kafka_topic,
            security_protocol=args.kafka_security_protocol,
            sasl_mechanism=args.kafka_sasl_mechanism,
            # Prefer env vars over CLI flags so secrets don't end up in shell history.
            sasl_username=args.kafka_sasl_username or os.environ.get("KAFKA_SASL_USERNAME"),
            sasl_password=args.kafka_sasl_password or os.environ.get("KAFKA_SASL_PASSWORD"),
        )

    if args.sink == "webhook":
        if not args.webhook_url:
            sys.exit("--webhook_url is required for --sink webhook.")

        return WebhookSink(args.webhook_url)
    raise ValueError(args.sink)

@dataclass
class Session:
    session_id: str
    user_id : str
    device: str
    referrer: str
    cart: list = field(default_factory=list)

async def run_session(session: Session, catalog: list, sink: Sink, 
                          min_think: float, max_think: float):
    state = "start"
    last_product = None

    while state != "exit":
        state = next_state(state)
        if state == "exit":
            break

        await asyncio.sleep(random.uniform(min_think, max_think))

        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": now_iso(),
            "session_id": session.session_id,
            "user_id": session.user_id,
            "device": session.device,
            "referrer": session.referrer
        }

        if state == "browse":
            event["page_type"] = "page_view"
            event["page"] = random.choice(["home"]+[f"category:{c}" for c in CATEGORIES])

        elif state == "view_product": 
            product = random(catalog)
            last_product = product
            event["event_type"] = "product_view"
            event["product_id"] = product["product_id"]
            event["product_name"] = product["name"]
            event["category"] = product["category"]
            event["price"] = product["price"]                

        elif state == "add_to_cart":
            product =last_product or random.choice(catalog)
            qty = random.randint(1,3)
            session.cart.apped({**product, "qty": qty})
            event["event_type"] = "add_to_cart"
            event["product_id"] = product["product_id"]
            event["price"] = product["price"]  
            event["quantity"] = qty
        
        elif state == "checkout":
            total = round(sum(i["price"] * i["qty"] for i in session.cart),2)
            event["event_type"] = "purchase"
            event["order_id"] = str(uuid.uuid4())
            event["items"] = [
                {
                    "product_id": i["product_id"],
                    "quantity": i["qty"],
                    "price": i["price"]
                }
                for i in session.cart
            ]
            event["order_value"] = total
            
        await sink.send(event)
async def session_spawner(catalog, users, sink, sessions_per_min: float,
                        duration: float, min_think: float, max_think: float):
    interval = 60/sessions_per_min
    tasks = []
    end_time = time.monotonic() + duration if duration else None
    
    while end_time is None or time.monotonic() < end_time:
        session = Session(
            session_id=str(uuid.uuid4()),
            user_id=random.choice(users),
            device=random.choice(DEVICES),
            referrer=random.choice(REFERRERS)
        )
        tasks.append(asyncio.create_task(
            run_session(session,catalog,sink, min_think, max_think)
        ))
        await asyncio.sleep(interval)
    if tasks:
        await asyncio.gather(*tasks)
                
                