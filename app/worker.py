"""
Kafka consumer for OCR / document parsing.

Flow:

Kafka input
    ->
consume request
    ->
choose PDF template
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
from copy import deepcopy

import structlog

from confluent_kafka import (
    Consumer,
    Producer,
    KafkaError
)

from . import db
from .config import get_settings
from .pipeline import (
    process,
    FileResolutionError
)


log = structlog.get_logger()

_running = True


# ---------------------------------------------------------
# PDF template -> YAML profile mapping
# ---------------------------------------------------------

PDF_PROFILE_BY_TEMPLATE = {

    "NT": "northern_trust_daily",
    "NORTHERN_TRUST": "northern_trust_daily",

    "SCHWAB": "charles_schwab",
    "CHARLES_SCHWAB": "charles_schwab",
}


DEFAULT_PDF_PROFILE = "northern_trust_daily"


# ---------------------------------------------------------
# Shutdown
# ---------------------------------------------------------

def _stop(*_):

    global _running

    _running = False


# ---------------------------------------------------------
# PDF profile selection
# ---------------------------------------------------------

def apply_pdf_profile(item: dict) -> str:
    """
    Select the PDF parser profile from the incoming message.

    Incoming field:

        "pdfTemplate": "NT"

    or:

        "pdfTemplate": "SCHWAB"

    Rules:

        missing / null / blank
            -> northern_trust_daily

        NT / NORTHERN_TRUST
            -> northern_trust_daily

        SCHWAB / CHARLES_SCHWAB
            -> charles_schwab

        unknown non-empty value
            -> fail instead of incorrectly parsing as NT
    """

    template_key = str(
        item.get("pdfTemplate") or ""
    ).strip().upper()

    # ---------------------------------------------
    # Missing template -> NT default
    # ---------------------------------------------

    if not template_key:

        profile_name = DEFAULT_PDF_PROFILE

    else:

        profile_name = PDF_PROFILE_BY_TEMPLATE.get(
            template_key
        )

        if not profile_name:

            raise ValueError(
                f"Unsupported pdfTemplate "
                f"'{template_key}'. "
                f"Supported values: "
                f"{sorted(PDF_PROFILE_BY_TEMPLATE.keys())}"
            )

    # ---------------------------------------------
    # Preserve any existing parser options
    # ---------------------------------------------

    parser_options = item.get(
        "parserOptions"
    )

    if not isinstance(
        parser_options,
        dict
    ):
        parser_options = {}

    pdf_options = parser_options.get(
        "pdf"
    )

    if not isinstance(
        pdf_options,
        dict
    ):
        pdf_options = {}

    # ---------------------------------------------
    # Explicit profile means pipeline does not
    # need automatic PDF profile detection.
    # ---------------------------------------------

    pdf_options["layout"] = "statement"
    pdf_options["profile"] = profile_name

    parser_options["pdf"] = pdf_options

    item["parserOptions"] = parser_options

    return profile_name


# ---------------------------------------------------------
# Determine PDF input
# ---------------------------------------------------------

def is_pdf_request(item: dict) -> bool:

    path = item.get("path")

    if (
        isinstance(path, str)
        and path.lower().endswith(".pdf")
    ):
        return True

    file_name = item.get("fileName")

    if (
        isinstance(file_name, str)
        and file_name.lower().endswith(".pdf")
    ):
        return True

    file_details = item.get(
        "fileImportDetails"
    )

    if isinstance(
        file_details,
        dict
    ):

        detail_file_name = file_details.get(
            "fileName"
        )

        if (
            isinstance(detail_file_name, str)
            and detail_file_name.lower().endswith(".pdf")
        ):
            return True

    return False


# ---------------------------------------------------------
# Kafka message decoding
# ---------------------------------------------------------

def split_message(
    raw: bytes
) -> list[dict]:

    """
    Kafka message may contain:

        {...}

    or:

        [{...}, {...}]
    """

    try:

        data = json.loads(
            raw
        )

    except (
        json.JSONDecodeError,
        UnicodeDecodeError
    ) as e:

        raise ValueError(
            f"message is not valid JSON: {e}"
        ) from e

    items = (
        data
        if isinstance(
            data,
            list
        )
        else [data]
    )

    bad = [
        type(x).__name__
        for x in items
        if not isinstance(
            x,
            dict
        )
    ]

    if bad:

        raise ValueError(
            f"expected JSON object(s), "
            f"got {bad}"
        )

    return items


# ---------------------------------------------------------
# Parser-message validation
# ---------------------------------------------------------

def is_parse_request(
    item: dict
) -> bool:

    return any(
        item.get(k)
        for k in (
            "path",
            "fileName",
            "superset",
            "fileImportDetails"
        )
    )


# ---------------------------------------------------------
# Worker
# ---------------------------------------------------------

def main():

    settings = get_settings()

    s = settings.kafka

    # Ensure DB schema exists
    db.init_schema()

    # -----------------------------------------------------
    # Kafka clients
    # -----------------------------------------------------

    consumer = Consumer(
        s.consumer_conf()
    )

    producer = Producer(
        s.producer_conf()
    )

    # -----------------------------------------------------
    # Partition assignment
    # -----------------------------------------------------

    def on_assign(
        consumer_instance,
        partitions
    ):

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

        log.info(
            "kafka.partitions.revoked",
            group=s.group_id,
            partitions=[
                {
                    "topic": p.topic,
                    "partition": p.partition
                }
                for p in partitions
            ]
        )

    consumer.subscribe(
        [s.topic],
        on_assign=on_assign,
        on_revoke=on_revoke
    )

    signal.signal(
        signal.SIGTERM,
        _stop
    )

    signal.signal(
        signal.SIGINT,
        _stop
    )

    log.info(
        "worker.started",
        topic=s.topic,
        group=s.group_id,
        brokers=s.bootstrap_servers,
        security=s.security_protocol,
        postgres=settings.postgres.safe(),
        result_topic=s.result_topic,
        dlq_topic=s.dlq_topic
    )

    # -----------------------------------------------------
    # Kafka producer callback
    # -----------------------------------------------------

    def delivered(
        err,
        message
    ):

        if err is not None:

            log.error(
                "kafka.produce.failed",
                topic=message.topic(),
                error=str(err)
            )

            return

        log.info(
            "kafka.produce.success",
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset()
        )

    # -----------------------------------------------------
    # Kafka send
    # -----------------------------------------------------

    def send(
        topic,
        obj,
        key
    ):

        if not topic:

            log.warning(
                "kafka.send.skipped",
                reason="topic_not_configured"
            )

            return

        producer.produce(
            topic,
            json.dumps(
                obj,
                default=str
            ).encode("utf-8"),
            key=key,
            on_delivery=delivered
        )

    # -----------------------------------------------------
    # Worker loop
    # -----------------------------------------------------

    while _running:

        msg = consumer.poll(
            1.0
        )

        if msg is None:
            continue

        # -------------------------------------------------
        # Kafka-level error
        # -------------------------------------------------

        if msg.error():

            if (
                msg.error().code()
                != KafkaError._PARTITION_EOF
            ):

                log.error(
                    "kafka.error",
                    err=str(
                        msg.error()
                    )
                )

            continue

        key = msg.key()

        where = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset()
        }

        # -------------------------------------------------
        # Kafka receipt
        # -------------------------------------------------

        log.info(
            "kafka.message.received",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=(
                key.decode(
                    errors="replace"
                )
                if key
                else None
            )
        )

        # -------------------------------------------------
        # Decode Kafka value
        # -------------------------------------------------

        try:

            items = split_message(
                msg.value()
            )

        except ValueError as e:

            log.error(
                "message.invalid",
                err=str(e),
                **where
            )

            send(
                s.dlq_topic,
                {
                    "error": str(e),
                    "retryable": False,
                    "original": (
                        msg.value().decode(
                            errors="replace"
                        )
                    )
                },
                key
            )

            items = []

        # -------------------------------------------------
        # Process every request in Kafka record
        # -------------------------------------------------

        for idx, item in enumerate(
            items
        ):

            log.info(
                "kafka.message.consumed",
                item=idx,
                fileSeqId=item.get(
                    "fileSeqId"
                ),
                path=item.get(
                    "path"
                ),
                pdfTemplate=item.get(
                    "pdfTemplate"
                ),
                **where
            )

            # ---------------------------------------------
            # Ignore messages not meant for parser
            # ---------------------------------------------

            if not is_parse_request(
                item
            ):

                log.warning(
                    "message.skipped_not_parse_request",
                    item=idx,
                    keys=sorted(
                        item
                    )[:15],
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
                # Detect physical PDF
                # -----------------------------------------

                source_path = item.get(
                    "path"
                )

                is_pdf_input = is_pdf_request(
                    item
                )

                # -----------------------------------------
                # IMPORTANT:
                #
                # Work on a copy.
                #
                # We add parserOptions only to this copy.
                # The original Kafka item remains unchanged
                # for the downstream result message.
                # -----------------------------------------

                processing_item = deepcopy(
                    item
                )

                # -----------------------------------------
                # Select YAML profile only for PDF
                # -----------------------------------------

                if is_pdf_input:

                    profile_name = apply_pdf_profile(
                        processing_item
                    )

                    log.info(
                        "pdf.profile.selected",
                        fileSeqId=item.get(
                            "fileSeqId"
                        ),
                        pdfTemplate=item.get(
                            "pdfTemplate"
                        ),
                        profile=profile_name
                    )

                # -----------------------------------------
                # Parse
                # -----------------------------------------

                log.info(
                    "pdf.processing.started",
                    fileSeqId=item.get(
                        "fileSeqId"
                    ),
                    source_path=source_path,
                    is_pdf=is_pdf_input
                )

                result = process(
                    processing_item
                )

                log.info(
                    "processing.completed",
                    fileSeqId=item.get(
                        "fileSeqId"
                    ),
                    records=result.get(
                        "records"
                    ),
                    batches=result.get(
                        "batches"
                    ),
                    status=result.get(
                        "status"
                    ),
                    output_path=result.get(
                        "outputPath"
                    )
                )

                # -----------------------------------------
                # Build downstream request from ORIGINAL
                # Kafka item.
                # -----------------------------------------

                downstream_message = dict(
                    item
                )

                output_path = result.get(
                    "outputPath"
                )

                # -----------------------------------------
                # PDF must produce JSON
                # -----------------------------------------

                if is_pdf_input:

                    if not output_path:

                        raise RuntimeError(
                            "PDF processing completed "
                            "but outputPath was not "
                            "returned. JSON file was "
                            "not created."
                        )

                    downstream_message[
                        "path"
                    ] = output_path

                    downstream_message[
                        "isPdfProcessorMessage"
                    ] = True

                    downstream_message[
                        "pdfFilePath"
                    ] = source_path

                    log.info(
                        "pdf.output.ready",
                        fileSeqId=item.get(
                            "fileSeqId"
                        ),
                        pdf_path=source_path,
                        json_path=output_path
                    )

                elif output_path:

                    downstream_message[
                        "path"
                    ] = output_path

                # -----------------------------------------
                # Send downstream
                # -----------------------------------------

                if s.result_topic:

                    log.info(
                        "pdf.result.message.sending",
                        result_topic=s.result_topic,
                        fileSeqId=(
                            downstream_message.get(
                                "fileSeqId"
                            )
                        ),
                        old_path=source_path,
                        new_path=(
                            downstream_message.get(
                                "path"
                            )
                        ),
                        isPdfProcessorMessage=(
                            downstream_message.get(
                                "isPdfProcessorMessage"
                            )
                        )
                    )

                    send(
                        s.result_topic,
                        downstream_message,
                        key
                    )

                    log.info(
                        "pdf.result.message.sent",
                        fileSeqId=(
                            downstream_message.get(
                                "fileSeqId"
                            )
                        )
                    )

                else:

                    log.warning(
                        "pdf.result.message.not_sent",
                        reason=(
                            "result_topic_not_configured"
                        ),
                        fileSeqId=item.get(
                            "fileSeqId"
                        )
                    )

            except Exception as e:

                log.exception(
                    "job.failed",
                    item=idx,
                    fileSeqId=item.get(
                        "fileSeqId"
                    ),
                    **where
                )

                send(
                    s.dlq_topic,
                    {
                        "error": repr(e),
                        "original": item,
                        "retryable": (
                            not isinstance(
                                e,
                                (
                                    FileResolutionError,
                                    ValueError
                                )
                            )
                        )
                    },
                    key
                )

        # -------------------------------------------------
        # Ensure result / DLQ messages are delivered
        # before committing consumed Kafka message.
        # -------------------------------------------------

        producer.flush(
            10
        )

        consumer.commit(
            message=msg,
            asynchronous=False
        )

    consumer.close()


if __name__ == "__main__":
    main()