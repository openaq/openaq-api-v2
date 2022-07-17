from io import BytesIO
from typing import List
from gzip import GzipFile
import datetime 
import logging
from operator import itemgetter, attrgetter
import json

import botocore
import boto3

s3_client = boto3.client('s3')
logs_client = boto3.client('logs')
logging.basicConfig(level=logging.INFO)


log_group_name = "api-dist-staging-log-group"
log_stream_name = "api-dist-staging-log-"

try:
    create_log_stream_response = logs_client.create_log_stream(
    logGroupName=log_group_name,
    logStreamName=log_stream_name
    )
except botocore.exceptions.ClientError as error:
    print('Error Creating log stream')
    raise error


def put_log(records, *sequence_token ):
    records = sorted(records,key=itemgetter('timestamp'))
    
    put_log_events_kwargs = {
        'logGroupName': log_group_name,
        'logStreamName': log_stream_name,
        'logEvents': records
    }

    try:
        if not sequence_token:
            print("no sequence token provided in args")
        else:
            print("sequence token provided in args")
            put_log_events_kwargs['sequenceToken'] = sequence_token[0]
            
        put_log_events_response = logs_client.put_log_events(**put_log_events_kwargs)
        print(put_log_events_response)
        sequence_token = put_log_events_response['nextSequenceToken']
        return put_log_events_response['nextSequenceToken']

    # Catch the missing or invalid sequence token error, this is one way to get the sequence token
    # The only place the sequence token is returned in the error is in the message, we have to parse out the token from the message.
    except (logs_client.exceptions.InvalidSequenceTokenException, logs_client.exceptions.DataAlreadyAcceptedException) as e:

        print(e.response['Error']['Code'])
        if e.response['Error']['Code'] == 'DataAlreadyAcceptedException':
            error_msg = e.response['Error']['Message']
            sequence_token = error_msg[len('The given batch of log events has already been accepted. The next batch can be sent with sequenceToken: '):]


        if e.response['Error']['Code'] == 'InvalidSequenceTokenException':
            error_msg = e.response['Error']['Message']
            sequence_token = error_msg[len('The given sequenceToken is invalid. The next expected sequenceToken is: '):]


        # Try again with the updated sequence token
        try:
            put_log_events_kwargs['sequenceToken'] = sequence_token
            put_log_events_response = logs_client.put_log_events(**put_log_events_kwargs)
            return put_log_events_response['nextSequenceToken']
        except Exception as e:
            print(e)
            print("Error putting log event")

    except Exception as e:
        print(e)
        print("Error putting log event")


def parse_log_file(key, bucket):
    """
    parses cloudfront s3 log in batches and puts to cloudwatch logs
    """
    records = []
    sequence_token = None
    records_byte_size = 0
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        bytestream = BytesIO(response['Body'].read())
        data = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.')
        raise e

    for line in data.strip().split("\n"):
        if not line.startswith("#"):
            try:
                split_line = line.split(sep="\t")
                timestamp = datetime.strptime(
                    '%s %s' % (split_line[0], split_line[1]),
                    '%Y-%m-%d %H:%M:%S'
                ).timestamp()

                time_in_ms = int(float(timestamp)*1000)

            except Exception as e:
                print(e)
                print("Failed to Covnert Time")

            # Check records array to see if we exceed payload limits if so, we need to publish the records, and clear out the records array:
            try:
                # size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
                line_count = len(records) +1
                bytes_overhead = line_count * 26

                # Get the total number of bytes for even in UTF-8
                line_encoded = line.strip().encode("utf-8", "ignore")

                # UTF 8 size of event
                line_byte_size = (line_encoded.__sizeof__())

                # UTF-8 size of records array
                records_byte_size = line_byte_size + records_byte_size

                # Total Payload Size
                payload_size = records_byte_size + bytes_overhead

            except Exception as e:
                print(e)
                print("Exception during utf8 conversion")

            if  payload_size >= 1048576 or line_count >= 10000 :
                try:
                    #payload size will be over 1 MB, or over the 10,000 record limit
                    print('payload OR records at limit, sending batch to CW Events ')
                    print(str(payload_size))
                    print(str(line_count))

                    # Write what we have now to CW Logs
                    if sequence_token is not  None:
                        print('sending put_log_event with sequence token')
                        sequence_token = put_log(records, sequence_token)
                    else:
                        sequence_token = put_log(records)
                        print('This is the returned sequence token',sequence_token )

                    #Clear out the records list
                    records = []

                    #reset Records_byte_size
                    records_byte_size = line_byte_size
                except Exception as e:
                    print(e)
                    print("Error sorting or sending records to CW Logs")


def handler(event, context):
    records = event["Records"]
    for record in records:
        body = json.loads(record["body"])
        data = body["Records"][0]["s3"]
        key = data["object"]["key"]
        bucket = data["bucket"]["name"]
        parse_log_file(key, bucket)