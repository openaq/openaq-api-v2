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
    timeout = settings.INGEST_TIMEOUT  # manual timeout for testing
    ascending = settings.FETCH_ASCENDING if 'ascending' not in event else event['ascending']
    pipeline_limit = settings.PIPELINE_LIMIT if 'pipeline_limit' not in event else event['pipeline_limit']
    realtime_limit = settings.REALTIME_LIMIT if 'realtime_limit' not in event else event['realtime_limit']
    metadata_limit = settings.METADATA_LIMIT if 'metadata_limit' not in event else event['metadata_limit']

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

    metadata = 0 if metadata is None else metadata[0]
    realtime = 0 if realtime is None else realtime[0]
    pipeline = 0 if pipeline is None else pipeline[0]
    logger.info(f"{metadata_limit}/{metadata} metadata, {realtime_limit}/{realtime} openaq, {pipeline_limit}/{pipeline} pipeline records pending")

    if metadata > 0 and metadata_limit > 0:
        cnt = 0
        while cnt < metadata and (time() - start_time) < timeout:
            cnt += load_metadata_db(metadata_limit, ascending)
            logger.info("loaded %s of %s metadata records, timer: %0.4f", cnt, metadata, time() - start_time)

    if realtime > 0 and realtime_limit > 0:
        cnt = 0
        while cnt < realtime and (time() - start_time) < timeout:
            cnt += load_db(realtime_limit, ascending)
            logger.info("loaded %s of %s fetch records, timer: %0.4f", cnt, realtime, time() - start_time)

    if pipeline > 0 and pipeline_limit > 0:
        cnt = 0
        while cnt < pipeline and (time() - start_time) < timeout:
            cnt += load_measurements_db(pipeline_limit, ascending)
            logger.info("loaded %s of %s pipeline records, timer: %0.4f", cnt, pipeline, time() - start_time)

    logger.info("done processing: %0.4f seconds", time() - start_time)
