"""
adapted from https://aws.amazon.com/blogs/mt/sending-cloudfront-standard-logs-to-cloudwatch-logs-for-analysis/
"""

from io import BytesIO
from typing import Dict
from gzip import GzipFile
from datetime import datetime
import logging
from operator import itemgetter
import json

import boto3
from pydantic import ValidationError

from .models import CloudfrontLog, CloudwatchLog
from .settings import settings

s3_client = boto3.client("s3")
logs_client = boto3.client("logs")

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
    level=settings.CF_LOGS_LOG_LEVEL.upper(),
    force=True,
)
logger = logging.getLogger("main")

# When debuging we dont want to debug these libraries
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log_group_name = f"openaq-api-{settings.ENV}-cf-access-log"
log_stream_name = f"openaq-api-{settings.ENV}-cf-access-log-stream"


def put_log(records: Dict, *sequence_token):
    records = [
        {"timestamp": int(k), "message": json.dumps(v)} for k, v in records.items()
    ]
    records = sorted(records, key=itemgetter("timestamp"))
    put_log_events_kwargs = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "logEvents": records,
    }
    try:
        if not sequence_token:
            logger.debug("no sequence token provided in args")
        else:
            put_log_events_kwargs["sequenceToken"] = sequence_token[0]

        put_log_events_response = logs_client.put_log_events(**put_log_events_kwargs)
        sequence_token = put_log_events_response["nextSequenceToken"]
        return put_log_events_response["nextSequenceToken"]

    except (
        logs_client.exceptions.InvalidSequenceTokenException,
        logs_client.exceptions.DataAlreadyAcceptedException,
    ) as e:
        logger.debug(e.response["Error"]["Code"])
        if e.response["Error"]["Code"] == "DataAlreadyAcceptedException":
            error_msg = e.response["Error"]["Message"]
            sequence_token = error_msg[
                len(
                    "The given batch of log events has already been accepted. The next batch can be sent with sequenceToken: "
                ) :
            ]

        if e.response["Error"]["Code"] == "InvalidSequenceTokenException":
            error_msg = e.response["Error"]["Message"]
            sequence_token = error_msg[
                len(
                    "The given sequenceToken is invalid. The next expected sequenceToken is: "
                ) :
            ]

        try:
            put_log_events_kwargs["sequenceToken"] = sequence_token
            put_log_events_response = logs_client.put_log_events(
                **put_log_events_kwargs
            )
            return put_log_events_response["nextSequenceToken"]
        except Exception as e:
            logger.error(f"Error putting log event: {e}")

    except Exception as e:
        logger.error(f"Error putting log event: {e}")


def parse_line(line: str) -> CloudfrontLog:
    """
    parses cloudfront log line to CloudfrontLog object for json serialization
    """
    args = line.split("\t")
    try:
        cloudfront_log = CloudfrontLog(
            date=args[0],
            time=args[1],
            location=args[2],
            bytes=args[3],
            request_ip=args[4],
            method=args[5],
            host=args[6],
            uri=args[7],
            status=args[8],
            refferer=args[9],
            user_agent=args[10],
            query_string=args[11],
            cookie=args[12],
            result_type=args[13],
            request_id=args[14],
            host_header=args[15],
            request_protocol=args[16],
            request_bytes=args[17],
            time_taken=args[18],
            xforwarded_for=args[19],
            ssl_protocol=args[20],
            ssl_cipher=args[21],
            response_result_type=args[22],
            http_version=args[23],
            fle_status=args[24],
            fle_encrypted_fields=args[25],
            c_port=args[26],
            time_to_first_byte=args[27],
            x_edge_detailed_result_type=args[28],
            sc_content_type=args[29],
            sc_content_len=args[30],
            sc_range_start=args[31],
            sc_range_end=args[32],
        )
    except ValidationError as e:
        logger.error(f"pydantic validation error: {e}")
    return cloudfront_log


def parse_log_file(key: str, bucket: str):
    """
    parses cloudfront s3 log in batches and puts to cloudwatch logs
    """
    records = {}
    sequence_token = None
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        bytestream = BytesIO(response["Body"].read())
        data = GzipFile(None, "rb", fileobj=bytestream).read().decode("utf-8")

    except Exception as e:
        logger.error(f"Error getting object {key} from bucket {bucket}. : {e}")
        raise e

    for line in data.strip().split("\n"):
        if not line.startswith("#"):
            try:
                split_line = line.split(sep="\t")

                timestamp = datetime.strptime(
                    "%s %s" % (split_line[0], split_line[1]), "%Y-%m-%d %H:%M:%S"
                ).timestamp()

                time_in_ms = int(float(timestamp) * 1000)

            except Exception as e:
                logger.error(f"Failed to Convert Time: {e}")

            # Check records array to see if we exceed payload limits if so, we need to publish the records, and clear out the records array:
            try:
                # size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
                line_count = len(records) + 1

                records_size = sum(
                    [len(json.dumps(r).encode("utf-8")) for r in records]
                )

            except Exception as e:
                logger.error(f"Exception during utf8 conversion: {e}")

            if records_size >= 900000 or line_count >= 9000:
                try:
                    logger.info(
                        f"payload OR records at limit, sending batch to CW Events payload: {records_size} line count: {line_count}"
                    )
                    if sequence_token is not None:
                        sequence_token = put_log(records, sequence_token)
                    else:
                        sequence_token = put_log(records)
                    records = []
                except Exception as e:
                    logger.error(f"Error sorting or sending records to CW Logs: {e}")

            try:
                message = parse_line(line)
                if not str(time_in_ms) in records.keys():
                    records[str(time_in_ms)] = {}
                records[str(time_in_ms)][str(message.status)] = (
                    records[str(time_in_ms)].get(str(message.status), 0) + 1
                )
            except Exception as e:
                logger.error(f"error adding Log Record to List: {e}")
    put_records_response = put_log(records)
    logger.info(put_records_response)


def handler(event, context):
    logger.debug(event)
    records = event["Records"]
    for record in records:
        body = json.loads(record["body"])
        data = body["Records"][0]["s3"]
        key = data["object"]["key"]
        bucket = data["bucket"]["name"]
        parse_log_file(key, bucket)
