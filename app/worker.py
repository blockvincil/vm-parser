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


def split_message(raw: bytes) -> list[dict]:
    """A message may carry one request object or a JSON array of them."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"message is not valid JSON: {e}") from e
    items = data if isinstance(data, list) else [data]
    bad = [type(x).__name__ for x in items if not isinstance(x, dict)]
    if bad:
        raise ValueError(f"expected JSON object(s), got {bad}")
    return items


def is_parse_request(item: dict) -> bool:
    return any(item.get(k) for k in ("path", "fileName", "superset", "fileImportDetails"))


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

    def delivered(err, m):                     # surface produce failures instead of losing them silently
        if err is not None:
            log.error("kafka.produce_failed", topic=m.topic(), err=str(err))

    def send(topic, obj, key):
        if topic:
            producer.produce(topic, json.dumps(obj, default=str).encode(), key=key, on_delivery=delivered)

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka.error", err=str(msg.error()))
            continue
        key, where = msg.key(), dict(partition=msg.partition(), offset=msg.offset())
        try:
            items = split_message(msg.value())
        except ValueError as e:                # not JSON / wrong shape: dead-letter the whole message
            log.error("message.invalid", err=str(e), **where)
            send(s.dlq_topic, {"error": str(e), "retryable": False,
                               "original": msg.value().decode(errors="replace")}, key)
            items = []
        for idx, item in enumerate(items):
            if not is_parse_request(item):
                log.warning("message.skipped_not_parse_request", item=idx, keys=sorted(item)[:15], **where)
                if not s.skip_non_parse_messages:
                    send(s.dlq_topic, {"error": "not a parse request (no path/fileName/superset)",
                                       "retryable": False, "original": item}, key)
                continue
            try:
                send(s.result_topic, process(item), key)
            except Exception as e:             # one bad item never blocks the others or the partition
                log.exception("job.failed", item=idx, fileSeqId=item.get("fileSeqId"), **where)
                send(s.dlq_topic, {"error": repr(e), "original": item,
                                   "retryable": not isinstance(e, (FileResolutionError, ValueError))}, key)
        producer.flush(10)
        consumer.commit(message=msg, asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    main()
