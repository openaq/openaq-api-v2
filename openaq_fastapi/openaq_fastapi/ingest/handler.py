import boto3
import logging
import psycopg2
from ..settings import settings
from .lcs import load_measurements_db, load_metadata_db
from .fetch import load_db
from time import time
#import math
import sys

from datetime import datetime, timezone

s3c = boto3.client("s3")

logger = logging.getLogger(__name__)
logging.basicConfig(
    format = '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level = settings.LOG_LEVEL.upper(),
    force = True,
)

def handler(event, context):
    logger.debug(event)
    records = event.get("Records")
    if records is not None:
        try:
            with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
                with connection.cursor() as cursor:
                    connection.set_session(autocommit=True)
                    for record in records:
                        bucket = record["s3"]["bucket"]["name"]
                        key = record["s3"]["object"]["key"]

                        lov2 = s3c.list_objects_v2(
                            Bucket=bucket, Prefix=key, MaxKeys=1
                        )
                        try:
                            last_modified = lov2["Contents"][0]["LastModified"]
                        except KeyError:
                            logger.error("could not get last modified time from obj")
                        last_modified = datetime.now().replace(
                            tzinfo=timezone.utc
                        )

                        cursor.execute(
                            """
                            INSERT INTO fetchlogs (key, last_modified)
                            VALUES(%s, %s)
                            ON CONFLICT (key) DO UPDATE
                            SET last_modified=EXCLUDED.last_modified,
                            completed_datetime=NULL RETURNING *;
                            """,
                            (key, last_modified,),
                        )
                        row = cursor.fetchone()
                        connection.commit()
                        logger.info(f"ingest-handler: {row}")
        except Exception as e:
            logger.warning(f"Exception: {e}")
    elif event.get("source") and event["source"] == "aws.events":
        cronhandler(event, context)
    else:
        logger.warning(f"ingest-handler: nothing to do: {event}")


def cronhandler(event, context):
    start_time = time()
    timeout = 15 * 60 ## manual timeout for testing
    ## some basic settings
    ascending = False if 'ascending' not in event else event['ascending']
    pipeline_limit = 10 if 'pipeline_limit' not in event else event['pipeline_limit']
    realtime_limit = 10 if 'realtime_limit' not in event else event['realtime_limit']
    metadata_limit = 10 if 'metadata_limit' not in event else event['metadata_limit']

    logger.info(f"Running cron job: {event['source']}, ascending: {ascending}")
    with psycopg2.connect(settings.DATABASE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*)
                FROM fetchlogs
                WHERE completed_datetime is null
                AND key ~*'stations';
                """,
            )
            metadata = cursor.fetchone()
            cursor.execute(
                """
                SELECT count(*)
                FROM fetchlogs
                WHERE completed_datetime is null
                AND key ~*'measures';
                """,
            )
            pipeline = cursor.fetchone()
            cursor.execute(
                """
                SELECT count(*)
                FROM fetchlogs
                WHERE completed_datetime is null
                AND key ~*'realtime';
                """,
            )
            realtime = cursor.fetchone()
            for notice in connection.notices:
                logger.debug(notice)

    logger.info(f"{metadata[0]} metadata, {realtime[0]} openaq, {pipeline[0]} pipeline records pending")

    if metadata is not None and metadata_limit > 0:
        val = int(metadata[0])
        cnt = 0
        while cnt < val and (time() - start_time) < timeout:
            cnt += load_metadata_db(metadata_limit, ascending)
            logger.info(f"loaded %s of %s metadata records, timer: %0.4f", cnt, val, time() - start_time)

    if realtime is not None and realtime_limit > 0:
        val = int(realtime[0])
        cnt = 0
        while cnt < val and (time() - start_time) < timeout:
            cnt += load_db(realtime_limit, ascending)
            logger.info(f"loaded %s of %s fetch records, timer: %0.4f", cnt, val, time() - start_time)

    if pipeline is not None and pipeline_limit > 0:
        val = int(pipeline[0])
        cnt = 0
        while cnt < val and (time() - start_time) < timeout:
            cnt += load_measurements_db(pipeline_limit, ascending)
            logger.info(f"loaded %s of %s pipeline records, timer: %0.4f", cnt, val, time() - start_time)

    logger.info("done processing: %0.4f seconds", time() - start_time)
