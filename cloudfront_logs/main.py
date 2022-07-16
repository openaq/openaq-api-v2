from typing import List
import csv
import gzip
import datetime 
import logging

import boto3

from .models import CloudfrontLog, CloudwatchLog

s3_client = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')
logging.basicConfig(level=logging.INFO)


def batch(rows: List[str], size: int):
    """
    takes rows and return list of lists of input size
    """
    return [
        rows[i:i + size]
        for i in range(0, len(rows), size)
    ]

def parse_log_file(key, bucket):
    """
    parses cloudfront s3 log in batches and puts to cloudwatch logs
    """
    s3_client.download_file(bucket, key, '/tmp/log.gz')
    events = []
    with gzip.open('/tmp/log.gz', 'rt') as logdata:
        reader = csv.DictReader(logdata)
        batches = batch(reader, 50)
        for batch in batches:
            for log in batch:
                timestamp = datetime.datetime.strptime("{date} {time}".format(**log), "%Y-%m-%d %H:%M:%S").timestamp()
                cloudfront_log = CloudfrontLog(**log)
                events.append(CloudwatchLog(timestamp=int(round(timestamp * 1000)), message=cloudfront_log.json()).json())
            cloudwatch_client.put_log_events(
                logGroupName='string',
                logStreamName='string',
                logEvents=[events],
                sequenceToken='string'
            )
            events.clear()


def lambda_handler(event, context):
    data = event["Records"][0]["s3"]
    key = data["object"]["key"]
    bucket = data["bucket"]["name"]
    parse_log_file(key, bucket)