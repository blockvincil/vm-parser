"""
Kafka consumer for OCR / document parsing.

Flow:

Kafka input
    ->
consume request
    ->
parse PDF/file
    ->
store DB batches
    ->
create JSON beside source file
    ->
send original request to result topic
with:
    path = generated JSON path
    isPdfProcessorMessage = true
"""

from __future__ import annotations

import json
import signal

import structlog

from confluent_kafka import (
    Consumer,
    Producer,
    KafkaError
)

from . import db
from .config import get_settings
from .pipeline import (process,FileResolutionError)


log = structlog.get_logger()

_running = True


def _stop(*_):
    global _running
    _running = False


def split_message(raw: bytes) -> list[dict]:
    try:
        data = json.loads(raw)

    except (json.JSONDecodeError, UnicodeDecodeError) as e:

        raise ValueError("message is not valid JSON: {e}")(e)

    items = (
        data
        if isinstance(data, list)
        else [data]
    )

    bad = [
        type(x).__name__
        for x in items
        if not isinstance(x, dict)
    ]

    if bad:
        raise ValueError(f"expected JSON object(s), "f"got {bad}")

    return items


def is_parse_request( item: dict) -> bool:

    return any(
        item.get(k)
        for k in ("path","fileName","superset","fileImportDetails" ))


def main():

    settings = get_settings()
    s = settings.kafka

    # Ensure DB schema exists
    db.init_schema()

    # -------------------------------------------------
    # Kafka clients
    # -------------------------------------------------

    consumer = Consumer(s.consumer_conf())
    producer = Producer(s.producer_conf() )

    # -------------------------------------------------
    # Partition assignment logs
    # -------------------------------------------------

    def on_assign(consumer_instance, partitions):

        log.info(
            "kafka.partitions.assigned",
            group=s.group_id,
            partitions=[
                {
                    "topic": p.topic,
                    "partition": p.partition
                }
                for p in partitions
            ]
        )

    def on_revoke(
        consumer_instance,
        partitions
    ):

        log.info("kafka.partitions.revoked",
            group=s.group_id,
            partitions=[
                {
                    "topic": p.topic,
                    "partition": p.partition
                }
                for p in partitions
            ]
        )

    consumer.subscribe( [s.topic],on_assign=on_assign,on_revoke=on_revoke )

    signal.signal( signal.SIGTERM,_stop )

    signal.signal( signal.SIGINT,_stop)

    log.info( "worker.started", topic=s.topic,group=s.group_id,brokers=s.bootstrap_servers,
        security=s.security_protocol,postgres=(settings.postgres.safe()), result_topic=s.result_topic,dlq_topic=s.dlq_topic)

    # -------------------------------------------------
    # Kafka producer callback
    # -------------------------------------------------

    def delivered(err, message):

        if err is not None:
            log.error( "kafka.produce.failed",topic=message.topic(),error=str(err) )
            return

        log.info("kafka.produce.success", topic=message.topic(),
            partition=(message.partition() ), offset=message.offset())

    # -------------------------------------------------
    # Kafka send utility
    # -------------------------------------------------

    def send(topic,obj,key):

        if not topic:
            log.warning( "kafka.send.skipped", reason="topic_not_configured")
            return

        producer.produce(topic, json.dumps( obj,default=str ).encode("utf-8"),
            key=key,on_delivery=delivered )

    # -------------------------------------------------
    # Worker loop
    # -------------------------------------------------

    while _running:

        msg = consumer.poll(  1.0 )

        if msg is None:
            continue

        # -------------------------------------------------
        # Kafka-level error
        # -------------------------------------------------

        if msg.error():

            if (msg.error().code() != KafkaError._PARTITION_EOF ):

                log.error( "kafka.error", err=str( msg.error()) )

            continue

        key = msg.key()

        where = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset()
        }

        # -------------------------------------------------
        # Raw Kafka receipt log
        # -------------------------------------------------

        log.info("kafka.message.received",topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=( key.decode(  errors="replace" ) if key else None )
        )

        # -------------------------------------------------
        # Decode message
        # -------------------------------------------------

        try:

            items = split_message( msg.value() )

        except ValueError as e:

            log.error( "message.invalid",err=str(e),**where)

            send( s.dlq_topic,
                {
                    "error": str(e),
                    "retryable": False,
                    "original": (msg.value().decode(errors="replace"))
                },
                key
            )

            items = []

        # -------------------------------------------------
        # Process every item in Kafka record
        # -------------------------------------------------

        for idx, item in enumerate(items ):

            # ---------------------------------------------
            # Full consumed-message log
            # ---------------------------------------------

            log.info( "kafka.message.consumed",item=idx,fileSeqId=item.get( "fileSeqId"),
                path=item.get("path" ), message=item, **where  )

            # ---------------------------------------------
            # Ignore messages not meant for parser
            # ---------------------------------------------

            if not is_parse_request( item ):

                log.warning( "message.skipped_not_parse_request",item=idx,
                    keys=sorted(  item)[:15],
                    **where
                )

                if not (
                    s.skip_non_parse_messages
                ):

                    send(
                        s.dlq_topic,
                        {
                            "error": (
                                "not a parse request "
                                "(no path/fileName/"
                                "superset/"
                                "fileImportDetails)"
                            ),
                            "retryable": False,
                            "original": item
                        },
                        key
                    )

                continue

            try:

                # -----------------------------------------
                # Determine whether input is PDF
                # -----------------------------------------

                source_path = item.get( "path" )

                is_pdf_input = ( isinstance( source_path,str  )
                    and source_path.lower().endswith(".pdf"))

                log.info( "pdf.processing.started", fileSeqId=item.get("fileSeqId"),
                    source_path=source_path,is_pdf=is_pdf_input)

                # -----------------------------------------
                # Parse
                # -----------------------------------------

                result = process(
                    item
                )

                log.info( "processing.completed", fileSeqId=item.get(   "fileSeqId"  ),
                    records=result.get(  "records" ),batches=result.get(  "batches" ),
                    status=result.get( "status"),output_path=result.get( "outputPath" ))

                # -----------------------------------------
                # Build downstream message
                #
                # IMPORTANT:
                # Copy original message.
                # Do not mutate incoming item.
                # -----------------------------------------

                downstream_message = dict(   item  )

                output_path = result.get(  "outputPath" )

                # -----------------------------------------
                # For physical/PDF processing we expect
                # generated JSON.
                # -----------------------------------------

                if is_pdf_input:

                    if not output_path:

                        raise RuntimeError(
                            "PDF processing completed "
                            "but outputPath was not "
                            "returned. JSON file was "
                            "not created."
                        )

                    downstream_message["path" ] = output_path

                    downstream_message["isPdfProcessorMessage" ] = True
                    downstream_message["pdfFilePath"] = source_path
                    log.info("pdf.output.ready", fileSeqId=item.get( "fileSeqId"),
                        pdf_path=source_path, json_path=output_path )

                elif output_path:

                    # Other physical formats can also
                    # produce a JSON file through
                    # DbJsonSink.
                    downstream_message["path"] = output_path

                # -----------------------------------------
                # Send same request to result topic
                # with JSON path
                # -----------------------------------------

                if s.result_topic:
                    log.info(
                        "pdf.result.message.sending",
                        result_topic=( s.result_topic ),
                        fileSeqId=(downstream_message.get("fileSeqId" ) ),
                        old_path=source_path,
                        new_path=(downstream_message.get("path") ),
                        isPdfProcessorMessage=( downstream_message.get( "isPdfProcessorMessage" ) )
                    )

                    send( s.result_topic,downstream_message, key )
                    log.info( "pdf.result.message.sent  ",downstream_message)
                else:

                    log.warning("pdf.result.message.not_sent",
                        reason=( "result_topic_not_configured"),
                        fileSeqId=item.get(   "fileSeqId"  ) )

            except Exception as e:

                log.exception( "job.failed", item=idx, fileSeqId=item.get("fileSeqId" ),**where )

                send( s.dlq_topic,
                    {
                        "error": repr(e),
                        "original": item,
                        "retryable": (not isinstance( e,( FileResolutionError,ValueError) ))
                    },
                    key
                )

        # -------------------------------------------------
        # Ensure produced result/DLQ messages are delivered
        # before committing consumed Kafka message.
        # -------------------------------------------------

        producer.flush( 10 )

        consumer.commit( message=msg, asynchronous=False )

    consumer.close()


if __name__ == "__main__":
    main()