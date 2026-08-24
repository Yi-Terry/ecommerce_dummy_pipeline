import argparse
import json 
import os 
from collections import Counter, defaultdict

from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()


def parse_args():
    p = argparse.ArgumentParser(description="Verify ecommerce events on a Kafka topic.")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="ecommerce_events")
    p.add_argument("--security-protocol", default="PLAINTEXT",
                    choices=["PLAINTEXT", "SASL_SSL", "SASL_PLAINTEXT"])
    p.add_argument("--sasl-mechanism", default="PLAIN")
    p.add_argument("--max-events", type=int, default=None,
                    help="Stop after this many events (default: run until Ctrl+C)")
    return p.parse_args()


def main():
    args = parse_args()
 
    kwargs = dict(
        bootstrap_servers=args.bootstrap,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        security_protocol=args.security_protocol,
        auto_offset_reset="earliest",  # read from the start of the topic
        group_id="verify_consumer",     # remembers position across runs
    )
    if args.security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        kwargs.update(
            sasl_mechanism=args.sasl_mechanism,
            sasl_plain_username=os.environ.get("KAFKA_SASL_USERNAME"),
            sasl_plain_password=os.environ.get("KAFKA_SASL_PASSWORD"),
        )
 
    consumer = KafkaConsumer(args.topic, **kwargs)
 
    event_type_counts = Counter()
    sessions_seen = defaultdict(list)
    n = 0
 
    print(f"Listening on topic '{args.topic}'... (Ctrl+C to stop)\n")
    try:
        for message in consumer:
            event = message.value
            n += 1
 
            event_type_counts[event.get("event_type")] += 1
            sessions_seen[event.get("session_id")].append(event.get("event_type"))
 
            print(f"[{n}] {event.get('event_type'):15s} "
                  f"session={event.get('session_id', '')[:8]}... "
                  f"user={event.get('user_id')}")
 
            if args.max_events and n >= args.max_events:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
 
    print("\n--- Summary ---")
    print(f"Total events: {n}")
    print(f"Distinct sessions: {len(sessions_seen)}")
    print("Event type counts:")
    for etype, count in event_type_counts.most_common():
        print(f"  {etype}: {count}")
 
    purchased_sessions = sum(1 for path in sessions_seen.values() if "purchase" in path)
    if sessions_seen:
        rate = 100 * purchased_sessions / len(sessions_seen)
        print(f"Sessions that purchased: {purchased_sessions}/{len(sessions_seen)} ({rate:.1f}%)")
 
 
if __name__ == "__main__":
    main()
 