"""Connectivity check for your Postgres and Kafka:  python -m app.check [--init-db]"""
from __future__ import annotations
import sys
from .config import get_settings


def main() -> int:
    s = get_settings()
    ok = True
    print(f"Postgres  {s.postgres.safe()}  schema={s.postgres.schema_}")
    try:
        from . import db
        print("  OK  ", db.ping().split(",")[0])
        if "--init-db" in sys.argv:
            db.init_schema()
            print(f"  OK   tables ensured in schema '{s.postgres.schema_}'")
    except Exception as e:
        ok = False
        print("  FAIL", repr(e))

    k = s.kafka
    print(f"Kafka     {k.bootstrap_servers}  security={k.security_protocol}"
          f"{' ' + k.sasl_mechanism if k.sasl_mechanism else ''}")
    try:
        from confluent_kafka.admin import AdminClient
        md = AdminClient(k.client_conf()).list_topics(timeout=10)
        print(f"  OK   {len(md.brokers)} broker(s), {len(md.topics)} topic(s)")
        for t in filter(None, (k.topic, k.result_topic, k.dlq_topic)):
            if t in md.topics:
                print(f"  OK   topic {t} ({len(md.topics[t].partitions)} partitions)")
            else:
                print(f"  WARN topic {t} not found (create it, or rely on broker auto-create)")
    except Exception as e:
        ok = False
        print("  FAIL", repr(e))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
