import boto3
import logging
import psycopg2
from ..settings import settings
from .lcs import (
    load_measurements_db,
    load_metadata_db,
    load_versions_db,
)
from .utils import (
    calculate_hourly_rollup_day,
    calculate_hourly_rollup_hour,
    calculate_hourly_rollup_stale,
    calculate_rollup_daily_stats,
    get_pending_rollup_days,
)
from .fetch import load_db
from time import time

from datetime import (
    date,
    datetime,
    timezone,
    timedelta,
)

s3c = boto3.client("s3")

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
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
                            logger.error("""
                            could not get last modified time from obj
                            """)
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
    elif event.get("method"):
        method_handler(event, context)
    elif event.get("source") and event["source"] == "aws.events":
        cronhandler(event, context)
    else:
        logger.warning(f"ingest-handler: nothing to do: {event}")


def rollup_handler(event, context):
    """
    This method is in place of custom events
    """
    minutes = 6  # could also be pulled from context
    # now = datetime.strptime(event.get('hour'), '%Y-%m-%d %H:%M:%S')
    now = datetime.now()
    # run the hourly method at around the 30 min mark
    hourly_time = now.replace(minute=30, second=0)
    # run the daily at around 45 minutes after midnight
    daily_time = now.replace(hour=0, minute=45, second=0)
    # how many pending days to do
    n_days = 1
    # should we run our hourly method
    if hourly_time >= (now - timedelta(minutes=minutes)) and hourly_time < now:
        method_handler({
            "method": "hourly_rollup",
            "hour": hourly_time,
        })
    # is it time to run our daily rollup?
    if daily_time >= (now - timedelta(minutes=minutes)) and daily_time < now:
        n_days = 0
        method_handler({
            "method": "daily_rollup",
            "day": daily_time,
        })
    # now run the cleanup method with the time left
    if n_days > 0:
        days = get_pending_rollup_days(n_days)
        for row in days:
            start_time = time()
            day = row[0]
            n = calculate_hourly_rollup_day(day)
            calculate_rollup_daily_stats(day)
            logger.info('Updated %s with %s sensor hours in %0.4f seconds',
                        day.strftime("%Y-%m-%d"), n, time() - start_time)


def method_handler(event, context=None):
    method = event.get("method")
    start_time = time()
    if method == "hourly_rollup":
        hour = event.get('hour')
        if hour is None:
            # sql function will truncate to hour
            # and hour ending would make it the last hour
            hour = datetime.now()
        n = calculate_hourly_rollup_hour(hour)
    elif method == "daily_rollup":
        day = event.get('day')
        if day is None:
            day = date.today() - timedelta(days=1)
        n = calculate_hourly_rollup_day(day)
        # update the stats/tracking table
        print(n)
        if n >= 23:
            calculate_rollup_daily_stats(day)
    elif method == "stale_rollup":
        n = calculate_hourly_rollup_stale()
    else:
        logger.warn(f'No method provided: {event}')
        return

    logger.info('Updated %s sensor hours with %s in %0.4f seconds',
                n, method, time() - start_time)


def cronhandler(event, context):
    start_time = time()
    timeout = settings.INGEST_TIMEOUT  # manual timeout for testing
    ascending = settings.FETCH_ASCENDING if 'ascending' not in event else event['ascending']
    pipeline_limit = settings.PIPELINE_LIMIT if 'pipeline_limit' not in event else event['pipeline_limit']
    realtime_limit = settings.REALTIME_LIMIT if 'realtime_limit' not in event else event['realtime_limit']
    metadata_limit = settings.METADATA_LIMIT if 'metadata_limit' not in event else event['metadata_limit']
    versions_limit = settings.VERSIONS_LIMIT if 'versions_limit' not in event else event['versions_limit']

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
            cursor.execute(
                """
                SELECT count(*)
                FROM fetchlogs
                WHERE completed_datetime is null
                AND key ~*'versions';
                """,
            )
            versions = cursor.fetchone()
            for notice in connection.notices:
                logger.debug(notice)

    metadata = 0 if metadata is None else metadata[0]
    versions = 0 if versions is None else versions[0]
    realtime = 0 if realtime is None else realtime[0]
    pipeline = 0 if pipeline is None else pipeline[0]
    logger.info(f"{metadata_limit}/{metadata} metadata, {realtime_limit}/{realtime} openaq, {pipeline_limit}/{pipeline} pipeline, {versions_limit}/{versions} versions records pending")

    # these exceptions are just a failsafe so that if something
    # unaccounted for happens we can still move on to the next
    # process. In case of this type of exception we will need to
    # fix it asap
    try:
        if metadata > 0 and metadata_limit > 0:
            cnt = 0
            while cnt < metadata and (time() - start_time) < timeout:
                cnt += load_metadata_db(metadata_limit, ascending)
                logger.info(
                    "loaded %s of %s metadata records, timer: %0.4f",
                    cnt, metadata, time() - start_time
                )
                if cnt == 0:
                    raise Exception('count is still zero after iteration')
    except Exception as e:
        logger.error(f"load metadata failed: {e}")

    try:
        if realtime > 0 and realtime_limit > 0:
            cnt = 0
            while cnt < realtime and (time() - start_time) < timeout:
                cnt += load_db(realtime_limit, ascending)
                logger.info(
                    "loaded %s of %s fetch records, timer: %0.4f",
                    cnt, realtime, time() - start_time
                )
                if cnt == 0:
                    raise Exception('count is still zero after iteration')
    except Exception as e:
        logger.error(f"load realtime failed: {e}")

    try:
        if versions > 0 and versions_limit > 0:
            cnt = 0
            while cnt < versions and (time() - start_time) < timeout:
                cnt += load_versions_db(versions_limit, ascending)
                logger.info(
                    "loaded %s of %s versions records, timer: %0.4f",
                    cnt, versions, time() - start_time
                )
                if cnt == 0:
                    raise Exception('count is still zero after iteration')
    except Exception as e:
        logger.error(f"load versions failed: {e}")

    try:
        if pipeline > 0 and pipeline_limit > 0:
            cnt = 0
            while cnt < pipeline and (time() - start_time) < timeout:
                cnt += load_measurements_db(pipeline_limit, ascending)
                logger.info(
                    "loaded %s of %s pipeline records, timer: %0.4f",
                    cnt, pipeline, time() - start_time
                )
                if cnt == 0:
                    raise Exception('count is still zero after iteration')
    except Exception as e:
        logger.error(f"load pipeline failed: {e}")

    logger.info("done processing: %0.4f seconds", time() - start_time)
