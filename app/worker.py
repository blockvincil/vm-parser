"""Kafka consumer for bd-ocr-flow. Manual commit after the job is durable -> at-least-once,
made effectively-once by the UNIQUE(file_seq_id, batch_no) + COMPLETED-skip in claim_job.
Scale horizontally: run N replicas in the same group_id (<= partition count)."""
from __future__ import annotations
import json
import signal
import structlog
from confluent_kafka import Consumer, Producer, KafkaError

from . import db
from .config import get_settings
from .pipeline import process, FileResolutionError

log = structlog.get_logger()
_running = True


def _stop(*_):
    global _running
    _running = False


def main():
    s = get_settings().kafka
    db.init_schema()
    consumer = Consumer(s.consumer_conf())
    producer = Producer(s.producer_conf())
    consumer.subscribe([s.topic])
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info("worker.started", topic=s.topic, group=s.group_id, brokers=s.bootstrap_servers,
             security=s.security_protocol, postgres=get_settings().postgres.safe())

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka.error", err=str(msg.error()))
            continue
        key = msg.key()
        try:
            payload = json.loads(msg.value())
            result = process(payload)
            if s.result_topic:
                producer.produce(s.result_topic, json.dumps(result, default=str).encode(), key=key)
        except Exception as e:                 # poison message or failed file -> DLQ, don't block partition
            log.exception("job.failed", offset=msg.offset())
            if s.dlq_topic:
                producer.produce(s.dlq_topic, json.dumps({
                    "error": repr(e), "retryable": not isinstance(e, (FileResolutionError, ValueError)),
                    "original": msg.value().decode(errors="replace")}).encode(), key=key)
        finally:
            producer.flush(10)
            consumer.commit(message=msg, asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    main()
