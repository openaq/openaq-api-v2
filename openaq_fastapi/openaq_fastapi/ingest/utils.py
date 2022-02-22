import io
import os
from pathlib import Path
import logging
from urllib.parse import unquote_plus
import gzip

import boto3
from io import StringIO
import psycopg2
import typer

from ..settings import settings

app = typer.Typer()

dir_path = os.path.dirname(os.path.realpath(__file__))

s3 = boto3.client("s3")

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


def get_logs_from_ids(ids):
    """Get the fetch logs based on fetchlogs_id"""
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fetchlogs_id
                , key
                , init_datetime
                , loaded_datetime
                , completed_datetime
                , last_message
                , last_modified
                FROM fetchlogs
                WHERE fetchlogs_id = ANY(%s)
                """,
                (ids,),
            )
            rows = cursor.fetchall()
            return rows


def get_logs_from_pattern(pattern: str, limit: int = 250):
    """Fetch all logs matching a pattern"""
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fetchlogs_id
                , key
                , init_datetime
                , loaded_datetime
                , completed_datetime
                , last_message
                , last_modified
                FROM fetchlogs
                WHERE key~*%s
                LIMIT %s
                """,
                (pattern, limit,),
            )
            rows = cursor.fetchall()
            return rows


def fix_units(value: str):
    """Clean up the units field. This was created to deal with mu vs micro issue in the current units list"""
    units = {
        "μg/m3": "µg/m³",
        "µg/m3": "µg/m³",
        "μg/m³": "µg/m³",
    }
    if value in units.keys():
        return units[value]
    else:
        return value


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


def get_object(
        key: str,
        bucket: str = settings.OPENAQ_ETL_BUCKET,
):
    key = unquote_plus(key)
    text = ''
    if bucket is not None and bucket != '':
        obj = s3.get_object(
            Bucket=bucket,
            Key=key,
        )
        body = obj['Body']
        if str.endswith(key, ".gz"):
            text = gzip.decompress(body.read()).decode('utf-8')
        else:
            text = body
    else:
        logger.info('attempting to load file locally {key}')
        if str.endswith(key, ".gz"):
            with gzip.open(key, 'rt') as f:
                for line in f:
                    text += line
        else:
            text = Path(key).read_text()

    return text



def put_object(
        data: str,
        key: str,
        bucket: str = settings.OPENAQ_ETL_BUCKET
):
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode='wb') as gz:
        with io.TextIOWrapper(gz, encoding='utf-8') as wrapper:
            wrapper.write(data)
    if settings.DRYRUN:
        filepath = os.path.join(bucket, key)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logger.debug(f"Dry Run: Writing file to local file in {filepath}")
        txt = open(f"{filepath}", "wb")
        txt.write(out.getvalue())
        txt.close()
    else:
        logger.info(f"Uploading file to {bucket}/{key}")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=out.getvalue(),
        )


def select_object(key: str):
    key = unquote_plus(key)
    output_serialization = None
    input_serialization = None

    if str.endswith(key, ".gz"):
        compression = "GZIP"
    else:
        compression = "NONE"

    if '.csv' in key:
        output_serialization = {
            'CSV': {}
        }
        input_serialization = {
            "CSV": {"FieldDelimiter": ","},
            "CompressionType": compression,
        }
    elif 'json' in key:
        output_serialization = {
            'JSON': {}
        }
        input_serialization = {
            "JSON": {"Type": "Document"},
            "CompressionType": compression,
        }

    content = ""
    logger.debug(f"Getting object: {key}, {output_serialization}")
    resp = s3.select_object_content(
        Bucket=settings.OPENAQ_ETL_BUCKET,
        Key=key,
        ExpressionType="SQL",
        Expression="""
            SELECT
            *
            FROM s3object
            """,
        InputSerialization=input_serialization,
        OutputSerialization=output_serialization,
    )
    for event in resp["Payload"]:
        if "Records" in event:
            content += event["Records"]["Payload"].decode("utf-8")
    return content


def load_errors_summary(days: int = 30):
    """Fetch any possible file errors"""
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH logs AS (
                SELECT init_datetime
                , CASE
                WHEN key~E'^realtime' THEN 'realtime'
                WHEN key~E'^lcs-etl-pipeline/measures' THEN 'pipeline'
                WHEN key~E'^lcs-etl-pipeline/station' THEN 'metadata'
                ELSE key
                END AS type
                , fetchlogs_id
                FROM fetchlogs
                WHERE last_message~*'^error'
                AND init_datetime > current_date - %s)
                SELECT type
                , init_datetime::date as day
                , COUNT(1) as n
                , MIN(init_datetime)::time as min_time
                , MAX(init_datetime)::time as max_time
                , MIN(fetchlogs_id) as fetchlogs_id
                FROM logs
                GROUP BY init_datetime::date, type
                ORDER BY init_datetime::date
                """,
                (days,),
            )
            rows = cursor.fetchall()
            return rows


def load_rejects_summary(days: int = 30):
    """Fetch any possible file errors"""
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH r AS (
                SELECT split_part(r->>'ingest_id', '-', 2) as source_id
                , split_part(r->>'ingest_id', '-', 1) as provider_id
                , fetchlogs_id
                FROM rejects
                WHERE fetchlogs_id IS NOT NULL
                AND t > current_date - %s)
                SELECT provider_id
                , r.source_id
                , fetchlogs_id
                , sensor_nodes_id
                , COUNT(1) as records
                FROM r
                LEFT JOIN sensor_nodes sn
                ON (r.source_id = sn.source_id
                AND r.provider_id = sn.source_name)
                GROUP BY provider_id
                , r.source_id
                , sensor_nodes_id
                , fetchlogs_id;
                """,
                (days,),
            )
            rows = cursor.fetchall()
            return rows


def load_errors_list(limit: int = 10):
    """Fetch any possible file errors"""
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fetchlogs_id
                , key
                , init_datetime
                , loaded_datetime
                , completed_datetime
                , last_message
                FROM fetchlogs
                WHERE last_message~*'^error'
                ORDER BY fetchlogs_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            return rows


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


# def load_success(cursor, key):
#     cursor.execute(
#         """
#         UPDATE fetchlogs
#         SET
#         last_message=%s,
#         loaded_datetime=clock_timestamp()
#         WHERE
#         key=%s
#         """,
#         (
#             str(cursor.statusmessage),
#             key,
#         ),
#     )


def load_success(cursor, keys, message: str = 'success'):
    cursor.execute(
        """
        UPDATE fetchlogs
        SET
        last_message=%s
        , completed_datetime=clock_timestamp()
        WHERE key=ANY(%s)
        """,
        (
            message,
            keys,
        ),
    )


def add_fetchlog(key: str):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        with connection.cursor() as cursor:
            connection.set_session(autocommit=True)
            cursor.execute(
                """
                INSERT INTO fetchlogs (key, last_modified)
                VALUES(%s, clock_timestamp())
                ON CONFLICT (key) DO UPDATE
                SET last_modified=EXCLUDED.last_modified,
                completed_datetime=NULL RETURNING *;
                """,
                (key,),
            )
            row = cursor.fetchone()
            connection.commit()
            return row


def mark_success(
        id: int = None,
        key: str = None,
        keys: list = None,
        ids: list = None,
        message: str = 'success',
        reset: bool = False,
):
    if id is not None:
        where = "fetchlogs_id = %s"
        param = id
    elif key is not None:
        where = "key=%s"
        param = key
    elif keys is not None:
        where = "key=ANY(%s)"
        param = keys
    elif ids is not None:
        where = "fetchlogs_id=ANY(%s)"
        param = ids
    else:
        logger.error('Failed to pass identifier')

    if reset:
        completed = 'NULL'
    else:
        completed = 'clock_timestamp'

    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            logger.info(f"Marking {where} / {param} as done, completed: {completed}")
            cursor.execute(
                f"""
                UPDATE fetchlogs
                SET
                last_message=%s
                , completed_datetime={completed}
                WHERE {where}
                """,
                (
                    message,
                    param,
                ),
    )


def crawl(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
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
