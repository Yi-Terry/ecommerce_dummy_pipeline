import argparse
import asyncio
import json
import logging
import random
import sys
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker()
logger = logging.getLogger("generator")

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

def build_user(n_users: int=2000):
    return  [f"U{i:06d}" for i in range(n_users)]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

TRANSITIONS = {
    "start": [("browse", 1.0)],
    "browse": [("view_product", 0.75), ("exit", 0.25)],
    "view_product": [("view_product", 0.30), ("add_to_cart", 0.30), ("exit", 0.40)],
    "add_to_cart": [("view_product", 0.25), ("checkout", 0.35), ("exit", 0.40)],
    "checkout": [("purchase", 0.75), ("exit", 0.25)],
    "purchase": [("exit", 1.0)]
}

def next_state(state: str) -> str:
    options, weights = zip(*TRANSITIONS[state])

    return random.choices(options, weights=weights, k=1)[0]

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

class DatabricksSink(Sink):
    """
    Writes events directly into a Databricks Delta table via SQL INSERT,
    using a Databricks SQL Warehouse. Buffers events and flushes in batches
    to avoid one network round trip per event.
    """
 
    def __init__(self, server_hostname: str, http_path: str, access_token: str,
                 catalog: str, schema: str, table: str, batch_size: int = 50):
        from databricks import sql  # lazy import, requires databricks-sql-connector
 
        self.connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )
        self.table = f"{catalog}.{schema}.{table}"
        self.batch_size = batch_size
        self.buffer = []
 
    async def send(self, event: dict):
        self.buffer.append(event)
        if len(self.buffer) >= self.batch_size:
            self._flush()
 
    def _flush(self):
        if not self.buffer:
            return
 
        rows = self.buffer
        self.buffer = []
 
        values_clause = ", ".join(["(?, ?, ?)"] * len(rows))
        params = []
        for event in rows:
            params.extend([
                event.get("event_id"),
                json.dumps(event),
                event.get("timestamp"),
            ])
 
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {self.table} (event_id, raw_json, event_timestamp) "
                f"VALUES {values_clause}",
                params,
            )
 
    async def close(self):
        self._flush()  # send whatever's left in the buffer
        self.connection.close()


def make_sink(args) -> Sink:
    if args.sink == "console":
        logger.info("Sink: console")
        return ConsoleSink()

    if args.sink == "file":
        if not args.output:
            sys.exit(" --ouput required for --sink file.")
        logger.info("Sink: file (path=%s)", args.output)
        return FileSink(args.output)

    if args.sink == "kafka":
        if not args.kafka_topic:
            sys.exit("--kafka-topic required for --sink kafka.")
        logger.info("Sink: kafka (bootstrap=%s, topic=%s, security_protocol=%s)",
                     args.kafka_bootstrap, args.kafka_topic, args.kafka_security_protocol)
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

        logger.info("Sink: webhook (url=%s)", args.webhook_url)
        return WebhookSink(args.webhook_url)

    if args.sink == "databricks":
        server_hostname = args.databricks_server_hostname or os.environ.get("DATABRICKS_SERVER_HOSTNAME")
        if not server_hostname:
            sys.exit("--databricks-server-hostname (or DATABRICKS_SERVER_HOSTNAME env var) is required for --sink databricks")
        http_path = args.databricks_http_path or os.environ.get("DATABRICKS_HTTP_PATH")
        if not http_path:
            sys.exit("--databricks-http-path (or DATABRICKS_HTTP_PATH env var) is required for --sink databricks")
        token = args.databricks_token or os.environ.get("DATABRICKS_TOKEN")
        if not token:
            sys.exit("--databricks-token (or DATABRICKS_TOKEN env var) is required for --sink databricks")
        logger.info("Sink: databricks (host=%s, table=%s.%s.%s, batch_size=%d)",
                     server_hostname, args.databricks_catalog, args.databricks_schema,
                     args.databricks_table, args.databricks_batch_size)
        return DatabricksSink(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=token,
            catalog=args.databricks_catalog,
            schema=args.databricks_schema,
            table=args.databricks_table,
            batch_size=args.databricks_batch_size,
        )

    raise ValueError(args.sink)

@dataclass
class Session:
    session_id: str
    user_id : str
    device: str
    referrer: str
    cart: list = field(default_factory=list)

@dataclass
class Stats:
    sessions_started: int = 0
    events_sent: int = 0
    purchases: int = 0

async def run_session(session: Session, catalog: list, sink: Sink,
                          min_think: float, max_think: float, stats: Stats):
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
            event["event_type"] = "page_view"
            event["page"] = random.choice(["home"]+[f"category:{c}" for c in CATEGORIES])

        elif state == "view_product": 
            product = random.choice(catalog)
            last_product = product
            event["event_type"] = "product_view"
            event["product_id"] = product["product_id"]
            event["product_name"] = product["name"]
            event["category"] = product["category"]
            event["price"] = product["price"]                

        elif state == "add_to_cart":
            product =last_product or random.choice(catalog)
            qty = random.randint(1,3)
            session.cart.append({**product, "qty": qty})
            event["event_type"] = "add_to_cart"
            event["product_id"] = product["product_id"]
            event["price"] = product["price"]  
            event["quantity"] = qty

        elif state == "checkout":
            event["event_type"] = "begin_checkout"
            event["cart_size"] = sum(i["qty"] for i in session.cart)
            event["cart_value"] = round(sum(i["price"] * i["qty"] for i in session.cart), 2)

        
        elif state == "purchase":
            total = round(sum(i["price"] * i["qty"] for i in session.cart), 2)
            event["event_type"] = "purchase"
            event["order_id"] = str(uuid.uuid4())
            event["items"] = [
                {"product_id": i["product_id"], "quantity": i["qty"], "price": i["price"]}
                for i in session.cart
            ]
            event["order_value"] = total
            stats.purchases += 1
            logger.info("Purchase: session=%s order=%s value=%.2f",
                        session.session_id[:8], event["order_id"][:8], total)

        await sink.send(event)
        stats.events_sent += 1
        logger.debug("Event sent: session=%s type=%s", session.session_id[:8], state)

async def session_spawner(catalog, users, sink, sessions_per_min: float,
                        duration: float, min_think: float, max_think: float, stats: Stats):
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
        stats.sessions_started += 1
        logger.info("Session started (#%d): user=%s device=%s referrer=%s",
                    stats.sessions_started, session.user_id, session.device, session.referrer)
        tasks.append(asyncio.create_task(
            run_session(session, catalog, sink, min_think, max_think, stats)
        ))
        await asyncio.sleep(interval)
    if tasks:
        await asyncio.gather(*tasks)

def parse_args():
    p = argparse.ArgumentParser(description="Simulate ecommerce user-session events.")
    p.add_argument("--sink", choices=["console", "file", "kafka", "webhook", "databricks"], default="console")
    p.add_argument("--output", help="Output file path (for --sink file)")
    p.add_argument("--kafka-bootstrap", default="localhost:9092",
                    help="e.g. localhost:9092 or pkc-xxxxx.confluent.cloud:9092")
    p.add_argument("--kafka-topic", default="ecommerce_events")
    p.add_argument("--kafka-security-protocol", default="PLAINTEXT",
                    choices=["PLAINTEXT", "SASL_SSL", "SASL_PLAINTEXT"],
                    help="Use SASL_SSL for managed Kafka (e.g. Confluent Cloud)")
    p.add_argument("--kafka-sasl-mechanism", default="PLAIN")
    p.add_argument("--kafka-sasl-username",
                    help="API key (or set KAFKA_SASL_USERNAME env var)")
    p.add_argument("--kafka-sasl-password",
                    help="API secret (or set KAFKA_SASL_PASSWORD env var — preferred)")
    p.add_argument("--webhook-url", help="URL to POST events to (for --sink webhook)")
    p.add_argument("--databricks-server-hostname",
                    help="Databricks SQL Warehouse hostname (or DATABRICKS_SERVER_HOSTNAME env var)")
    p.add_argument("--databricks-http-path",
                    help="Databricks SQL Warehouse HTTP path (or DATABRICKS_HTTP_PATH env var)")
    p.add_argument("--databricks-token",
                    help="Databricks access token (or DATABRICKS_TOKEN env var — preferred)")
    p.add_argument("--databricks-catalog", default="workspace")
    p.add_argument("--databricks-schema", default="bronze")
    p.add_argument("--databricks-table", default="ecommerce_events")
    p.add_argument("--databricks-batch-size", type=int, default=50)
    p.add_argument("--sessions-per-min", type=float, default=30.0,
                    help="Rate of new sessions starting")
    p.add_argument("--duration", type=float, default=60.0,
                    help="How long to run, in seconds (0 = run forever)")
    p.add_argument("--min-think", type=float, default=0.5, help="Min seconds between actions")
    p.add_argument("--max-think", type=float, default=3.0, help="Max seconds between actions")
    p.add_argument("--n-products", type=int, default=200)
    p.add_argument("--n-users", type=int, default=2000)
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    p.add_argument("--log-level", default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                    help="Logging verbosity (DEBUG logs every event sent)")
    return p.parse_args()

                
async def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    logger.info("Generating catalog (%d products) and users (%d)", args.n_products, args.n_users)
    catalog = build_catalog(args.n_products)
    users = build_user(args.n_users)
    sink = make_sink(args)
    stats = Stats()

    logger.info("Starting run: sessions_per_min=%s duration=%ss",
                args.sessions_per_min, args.duration if args.duration else "infinite")
    start = time.monotonic()
    try:
        await session_spawner(
            catalog, users, sink,
            sessions_per_min=args.sessions_per_min,
            duration=args.duration,
            min_think=args.min_think,
            max_think=args.max_think,
            stats=stats,
        )
    finally:
        await sink.close()
        elapsed = time.monotonic() - start
        logger.info("Run complete in %.1fs: sessions=%d events=%d purchases=%d",
                    elapsed, stats.sessions_started, stats.events_sent, stats.purchases)



if __name__ == "__main__":
    asyncio.run(main())