import gzip
import io
import json
import os
import logging
import time
from datetime import datetime, timedelta

import boto3
import psycopg2
import typer

from ..settings import settings
from .utils import (
    StringIteratorIO,
    clean_csv_value,
    get_query,
    load_fail,
    load_success,
)

app = typer.Typer()

dir_path = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)

FETCH_BUCKET = settings.FETCH_BUCKET
s3 = boto3.resource("s3")


def parse_json(j, key: str = None):
    location = j.pop("location", None)
    value = j.pop("value", None)
    unit = j.pop("unit", None)
    parameter = j.pop("parameter", None)
    country = j.pop("country", None)
    city = j.pop("city", None)
    source_name = j.pop("sourceName", None)
    date = j["date"]["utc"]
    j.pop("date", None)
    source_type = j.pop("sourceType", None)
    mobile = j.pop("mobile", None)
    avpd = j.pop("averagingPeriod", None)
    avpd_unit = avpd_value = None
    if avpd is not None:
        avpd_unit = avpd.pop("unit", None)
        avpd_value = avpd.pop("value", None)
    if (
        "coordinates" in j
        and "longitude" in j["coordinates"]
        and "latitude" in j["coordinates"]
    ):
        c = j.pop("coordinates")
        coords = "".join(
            (
                "SRID=4326;POINT(",
                str(c["longitude"]),
                " ",
                str(c["latitude"]),
                ")",
            )
        )
    else:
        coords = None

    data = json.dumps(j)

    row = [
        location,
        value,
        unit,
        parameter,
        country,
        city,
        data,
        source_name,
        date,
        coords,
        source_type,
        mobile,
        avpd_unit,
        avpd_value,
    ]
    linestr = "\t".join(map(clean_csv_value, row)) + "\n"
    return linestr


def create_staging_table(cursor):
    cursor.execute(get_query("fetch_staging.sql"))


def copy_data(cursor, key):
    obj = s3.Object(FETCH_BUCKET, key)
    # This should not be checked here,
    # if we ask it to copy data it should do that
    # if we want to prevent duplicate attemps we should
    # we should check earlier or better yet,
    # mark the file as checked out when we pull the key down
    # if check_if_done(cursor, key):
    #    return None
    # we are also removing the try/catch
    # if it fails we want to deal with it elsewhere
    logger.debug(f"Copying data for {key}")
    with gzip.GzipFile(fileobj=obj.get()["Body"]) as gz:
        f = io.BufferedReader(gz)
        iterator = StringIteratorIO(
            (parse_json(json.loads(line)) for line in f)
        )
        query = """
        COPY tempfetchdata (
        location,
        value,
        unit,
        parameter,
        country,
        city,
        data,
        source_name,
        datetime,
        coords,
        source_type,
        mobile,
        avpd_unit,
        avpd_value
        ) FROM STDIN;
        """
        cursor.copy_expert(query, iterator)


def copy_file(cursor, file):
    with gzip.GzipFile(file) as gz:
        f = io.BufferedReader(gz)
        iterator = StringIteratorIO(
            (parse_json(json.loads(line)) for line in f)
        )
        try:
            query = get_query("fetch_copy.sql")
            cursor.copy_expert(query, iterator)
            print("status:", cursor.statusmessage)
            # load_success(cursor, file)

        except Exception as e:
            load_fail(cursor, file, e)



def process_data(cursor):
    # print(1)
    # query = get_query("fetch_ingest1.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # print(2)
    # query = get_query("fetch_ingest2.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # print(3)
    # query = get_query("fetch_ingest3.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # print(4)
    # query = get_query("fetch_ingest4.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # print(5)
    # query = get_query("fetch_ingest5.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # print(6)
    # query = get_query("fetch_ingest6.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # print(7)
    # query = get_query("fetch_ingest7.sql")
    # cursor.execute(query)
    # print(cursor.statusmessage)
    # results = cursor.fetchone()
    query = get_query("fetch_ingest_full.sql")
    cursor.execute(query)
    # print(cursor.statusmessage)
    # results = cursor.fetchone()
    # print(f"{results}")
    # if results:
    #    mindate, maxdate = results
    #    print(f"{mindate} {maxdate}")
    #    return mindate, maxdate
    return None, None


def filter_data(cursor):
    query = get_query("fetch_filter.sql")
    cursor.execute(query)
    print(f"Deleted {cursor.rowcount} rows.")
    print(cursor.statusmessage)
    results = cursor.fetchone()
    print(f"{results}")
    if results:
        mindate, maxdate = results
        return mindate, maxdate
    return None, None


def update_rollups(cursor, mindate, maxdate):
    return None
    if mindate is not None and maxdate is not None:
        print(
            f"Updating rollups from {mindate.isoformat()}"
            f" to {maxdate.isoformat()}"
        )
        cursor.execute(
            get_query(
                "update_rollups.sql",
                mindate=mindate.isoformat(),
                maxdate=maxdate.isoformat(),
            )
        )
        print(cursor.statusmessage)
    else:
        print("could not get date range, skipping rollup update")


@app.command()
def load_fetch_file(file: str):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            create_staging_table(cursor)
            copy_file(cursor, file)
            min_date, max_date = filter_data(cursor)
            mindate, maxdate = process_data(cursor)
            update_rollups(cursor, mindate=mindate, maxdate=maxdate)
            connection.commit()


@app.command()
def load_fetch_day(day: str):
    start = time.time()
    conn = boto3.client("s3")
    prefix = f"realtime-gzipped/{day}"
    keys = []
    try:
        for f in conn.list_objects(Bucket=FETCH_BUCKET, Prefix=prefix)[
            "Contents"
        ]:
            # print(f['Key'])
            keys.append(f["Key"])
    except Exception as e:
        print(f"no data found for {day} Exception:{e}")
        return None

    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=False)
        with connection.cursor() as cursor:
            create_staging_table(cursor)
            for key in keys:
                copy_data(cursor, key)
            print(f"All data copied {time.time()-start}")
            filter_data(cursor)
            mindate, maxdate = process_data(cursor)
            update_rollups(cursor, mindate=mindate, maxdate=maxdate)
            connection.commit()


def load_prefix(prefix):
    conn = boto3.client("s3")
    for f in conn.list_objects(Bucket=FETCH_BUCKET, Prefix=prefix)["Contents"]:
        print(f["Key"])
        load_fetch_file(f["Key"])


@app.command()
def load_range(
    start: datetime = typer.Argument(
        (datetime.utcnow().date() - timedelta(days=3)).isoformat()
    ),
    end: datetime = typer.Argument(datetime.utcnow().date().isoformat()),
):
    print(
        f"Loading data from {start.date().isoformat()}"
        f" to {end.date().isoformat()}"
    )

    step = timedelta(days=1)
    while start <= end:
        load_fetch_day(f"{start.date().isoformat()}")
        start += step


def submit_file_error(ids, e):
    """Update the log to reflect the error and prevent a retry"""
    logger.error(e)
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fetchlogs
                SET completed_datetime = clock_timestamp()
                , last_message = %s
                WHERE fetchlogs_id = ANY(%s)
                """,
                (f"ERROR: {e}", ids,),
            )


@app.command()
def load_db(limit: int = 50, ascending: bool = False):
    order = 'ASC' if ascending else 'DESC'
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT key
                ,last_modified
                ,fetchlogs_id
                FROM fetchlogs
                WHERE key~E'^realtime-gzipped/.*\\.ndjson.gz$' AND
                completed_datetime is null
                ORDER BY last_modified {order} nulls last
                LIMIT %s
                ;
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            keys = [r[0] for r in rows]
            if len(keys) > 0:
                try:
                    load_realtime(keys)
                except Exception as e:
                    # catch and continue to next page
                    ids = [r[2] for r in rows]
                    logger.error(f"""
                    Error processing realtime files: {e}, {ids}
                    """)
                    submit_file_error(ids, e)
                finally:
                    connection.commit()

            return len(keys)


def load_realtime(keys):
    # create a connection and share for all keys
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            # create all the data staging table
            create_staging_table(cursor)
            # now copy all the data
            for key in keys:
                copy_data(cursor, key)
            # finally process the data as one
            process_data(cursor)
            # we are outputing some stats
            for notice in connection.notices:
                print(notice)
            # mark files as done
            load_success(cursor, keys)
            # close and commit
            connection.commit()


if __name__ == "__main__":
    app()
