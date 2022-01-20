import io
import os
from pathlib import Path
import logging

import boto3
from io import StringIO
import psycopg2
import typer

from ..settings import settings

app = typer.Typer()

dir_path = os.path.dirname(os.path.realpath(__file__))


FETCH_BUCKET = settings.OPENAQ_FETCH_BUCKET
s3 = boto3.resource("s3")
s3c = boto3.client("s3")

logger = logging.getLogger(__name__)


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter):
        self._iter = iter
        self._buff = ""

    def readable(self):
        return True

    def _read1(self, n=None):
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n=None):
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)


def clean_csv_value(value):
    if value is None or value == "":
        return r"\N"
    return str(value).replace("\n", "\\n").replace("\t", " ")


def get_query(file, **params):
    logger.debug("get_query: {file}, params: {params}")
    query = Path(os.path.join(dir_path, file)).read_text()
    if params is not None and len(params) >= 1:
        query = query.format(**params)
    return query


def check_if_done(cursor, key):
    cursor.execute(
        """
        SELECT 1 FROM fetchlogs
        WHERE key=%s
        AND completed_datetime IS NOT NULL
        """,
        (key,),
    )
    rows = cursor.rowcount
    print(f"Rows: {rows}")
    if rows >= 1:
        print("data file already loaded")
        return True

    cursor.execute(
        """
        INSERT INTO fetchlogs (key, init_datetime)
        VALUES(
            %s,
            clock_timestamp()
        ) ON CONFLICT (key)
        DO UPDATE
        SET init_datetime=clock_timestamp();
        INSERT INTO ingestfiles (key)
        VALUES (%s);
        """,
        (
            key,
            key,
        ),
    )
    return False


def load_fail(cursor, key, e):
    print("full copy failed", key, e)
    cursor.execute(
        """
        UPDATE fetchlogs
        SET
        last_message=%s
        WHERE
        key=%s
        """,
        (
            str(e),
            key,
        ),
    )


def load_success(cursor, key):
    cursor.execute(
        """
        UPDATE fetchlogs
        SET
        last_message=%s,
        loaded_datetime=clock_timestamp()
        WHERE
        key=%s
        """,
        (
            str(cursor.statusmessage),
            key,
        ),
    )


def crawl(bucket, prefix):
    paginator = s3c.get_paginator("list_objects_v2")
    print(settings.DATABASE_WRITE_URL)
    f = StringIO()
    cnt = 0
    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={"PageSize": 1000},
    ):
        cnt += 1
        print(".", end="")
        try:
            contents = page["Contents"]
        except KeyError:
            print("Done")
            break
        for obj in contents:
            key = obj["Key"]
            last_modified = obj["LastModified"]
            if key.endswith('.gz'):
                f.write(f"{key}\t{last_modified}\n")
                print(key)
    f.seek(0)
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.copy_expert(
                """
                    CREATE TEMP TABLE staging
                    (key text, last_modified timestamptz);
                    COPY staging(key,last_modified) FROM stdin;
                    INSERT INTO fetchlogs(key,last_modified)
                    SELECT * FROM staging
                    WHERE last_modified>'2021-01-10'::timestamptz
                    ON CONFLICT (key) DO
                        UPDATE SET
                            last_modified=EXCLUDED.last_modified;
                """,
                f,
            )


def crawl_lcs():
    crawl(settings.OPENAQ_ETL_BUCKET, "lcs-etl-pipeline/")


def crawl_fetch():
    crawl(settings.OPENAQ_FETCH_BUCKET, "realtime-gzipped/")
