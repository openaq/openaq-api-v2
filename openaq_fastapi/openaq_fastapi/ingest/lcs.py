import os
import logging
from datetime import datetime, timezone
import dateparser
import pytz
import orjson
import csv
from time import time
from urllib.parse import unquote_plus
import warnings

import boto3
import psycopg2
import typer
from io import StringIO
from ..settings import settings
from .utils import get_query, clean_csv_value, StringIteratorIO

s3 = boto3.resource("s3")
s3c = boto3.client("s3")

app = typer.Typer()
dir_path = os.path.dirname(os.path.realpath(__file__))

FETCH_BUCKET = settings.OPENAQ_ETL_BUCKET
logger = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)

class LCSData:
    def __init__(
        self, page=None, key=None, st=datetime.now().replace(tzinfo=pytz.UTC)
    ):
        logger.debug(f"Loading data with {len(page)} pages")
        self.sensors = []
        self.systems = []
        self.nodes = []
        self.keys = []
        self.page = page
        self.st = st
        if key is not None:
            self.page = [
                {
                    "Key": key,
                    "LastModified": datetime.now().replace(tzinfo=pytz.UTC),
                }
            ]

    def sensor(self, j, system_id):
        for s in j:
            sensor = {}
            metadata = {}
            sensor["ingest_sensor_systems_id"] = system_id
            for key, value in s.items():
                key = str.replace(key, "sensor_", "")
                if key == "id":
                    sensor["ingest_id"] = value
                elif key == "measurand_parameter":
                    sensor["measurand"] = value
                elif key == "measurand_unit":
                    sensor["units"] = value
                else:
                    metadata[key] = value
            sensor["metadata"] = orjson.dumps(metadata).decode()
            self.sensors.append(sensor)

    def system(self, j, node_id):
        for s in j:
            system = {}
            metadata = {}
            if "sensor_system_id" in s:
                id = s["sensor_system_id"]
            else:
                id = node_id
            system["ingest_sensor_nodes_id"] = node_id
            system["ingest_id"] = id
            for key, value in s.items():
                key = str.replace(key, "sensor_system_", "")
                if key == "sensors":
                    self.sensor(value, id)
                else:
                    metadata[key] = value
            system["metadata"] = orjson.dumps(metadata).decode()
            self.systems.append(system)

    def node(self, j):
        node = {}
        metadata = {}
        if "sensor_node_id" in j:
            id = j["sensor_node_id"]
        else:
            return None
        for key, value in j.items():
            key = str.replace(key, "sensor_node_", "")
            if key == "id":
                node["ingest_id"] = value
            elif key in ["site_name", "source_name", "ismobile"]:
                node[key] = value
            elif key == "geometry":
                try:
                    lon = float(value[0])
                    lat = float(value[1])
                    if lon != 0 and lat != 0:
                        node[
                            "geom"
                        ] = f"SRID=4326;POINT({value[0]} {value[1]})"
                    else:
                        node["geom"] = None
                except Exception:
                    node["geom"] = None
            elif key == "sensor_systems":
                self.system(value, id)
            else:
                metadata[key] = value
        node["metadata"] = orjson.dumps(metadata).decode()
        self.nodes.append(node)

    def get_station(self, key):
        logger.debug(f"get_station - {key}")
        if str.endswith(key, ".gz"):
            compression = "GZIP"
        else:
            compression = "NONE"
        # Removed the try block because getting the data is the whole
        # purpose of this function and we should not continue without it
        # if we want to check for specific errors we could do that,
        # but than rethrow
        resp = s3c.select_object_content(
            Bucket=FETCH_BUCKET,
            Key=key,
            ExpressionType="SQL",
            Expression="SELECT * FROM s3object",
            InputSerialization={
                "JSON": {"Type": "Document"},
                "CompressionType": compression,
            },
            OutputSerialization={"JSON": {}},
        )
        for event in resp["Payload"]:
            if "Records" in event:
                records = event["Records"]["Payload"].decode("utf-8")
                self.node(orjson.loads(records))


    def load_data(self):
        logger.debug(f"load_data: {self.keys}")
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:
                start_time = time()
                self.create_staging_table(cursor)

                write_csv(
                    cursor, self.keys, "keys", ["key", "last_modified", "fetchlogs_id",],
                )
                # update by id instead of key due to matching issue
                cursor.execute(
                    """
                    UPDATE fetchlogs
                    SET loaded_datetime = clock_timestamp()
                    , last_message = 'load_data'
                    WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM keys)
                    """
                )
                connection.commit()
                write_csv(
                    cursor,
                    self.nodes,
                    "ms_sensornodes",
                    [
                        "ingest_id",
                        "site_name",
                        "source_name",
                        "ismobile",
                        "geom",
                        "metadata",
                    ],
                )
                write_csv(
                    cursor,
                    self.systems,
                    "ms_sensorsystems",
                    ["ingest_id", "ingest_sensor_nodes_id", "metadata",],
                )
                write_csv(
                    cursor,
                    self.sensors,
                    "ms_sensors",
                    [
                        "ingest_id",
                        "ingest_sensor_systems_id",
                        "measurand",
                        "units",
                        "metadata",
                    ],
                )
                connection.commit()

                self.process_data(cursor)

                cursor.execute(
                    """
                    UPDATE fetchlogs
                    SET completed_datetime = clock_timestamp()
                    WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM keys)
                    """
                )

                connection.commit()
                logger.info("load_data: files: %s; time: %0.4f", len(self.keys), time() - start_time)
                for notice in connection.notices:
                    logger.debug(notice)

    def process_data(self, cursor):
        query = get_query("lcs_ingest_nodes.sql")
        cursor.execute(query)

        query = get_query("lcs_ingest_systems.sql")
        cursor.execute(query)

        query = get_query("lcs_ingest_sensors.sql")
        cursor.execute(query)

    def create_staging_table(self, cursor):
        cursor.execute(get_query("lcs_staging.sql"))

    def get_metadata(self):
        hasnew = False
        with psycopg2.connect(settings.DATABASE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:

                for obj in self.page:

                    key = obj["Key"]
                    id = obj["id"]
                    last_modified = obj["LastModified"]
                    logger.debug(f"checking fetchlog again: {key} {last_modified}")
                    # if last_modified > self.st:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM fetchlogs
                        WHERE fetchlogs_id=%s
                        AND completed_datetime IS NOT NULL
                        """,
                        (id,),
                    )
                    rows = cursor.rowcount
                    logger.debug(f"get_metadata:rows - {rows}")
                    if rows < 1:
                        try:
                            self.get_station(key)
                            self.keys.append(
                                {"key": key, "last_modified": last_modified, "fetchlogs_id": id}
                            )
                            hasnew = True
                        except Exception as e:
                            # catch and continue to next page
                            logger.error(f"Could not process file: {id}: {key}")

                if hasnew:
                    logger.debug(f"get_metadata:hasnew - {self.keys}")
                    self.load_data()
                for notice in connection.notices:
                    logger.debug(notice)


def write_csv(cursor, data, table, columns):
    fields = ",".join(columns)
    sio = StringIO()
    writer = csv.DictWriter(sio, columns)
    writer.writerows(data)
    sio.seek(0)
    cursor.copy_expert(
        f"""
        copy {table} ({fields}) from stdin with csv;
        """,
        sio,
    )
    logger.debug(f"table: {table}; rowcount: {cursor.rowcount}")


def load_metadata_bucketscan(count=100):
    paginator = s3c.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=FETCH_BUCKET,
        Prefix="lcs-etl-pipeline/stations",
        PaginationConfig={"PageSize": count},
    ):
        try:
            contents = page["Contents"]
            data = LCSData(contents)
            data.get_metadata()
        except KeyError:
            break


def load_metadata_db(count=250, ascending: bool = False):
    order = 'ASC' if ascending else 'DESC'
    with psycopg2.connect(settings.DATABASE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT key
                , last_modified
                , fetchlogs_id
                FROM fetchlogs
                WHERE key~'lcs-etl-pipeline/stations/'
                AND completed_datetime is null
                ORDER BY last_modified {order} nulls last
                LIMIT %s;
                """,
                (count,),
            )
            rows = cursor.fetchall()
            rowcount = cursor.rowcount
            contents = []
            for row in rows:
                contents.append(
                    {"Key": unquote_plus(row[0]), "LastModified": row[1],"id": row[2],}
                )
            for notice in connection.notices:
                logger.debug(notice)
    if len(contents) > 0:
        data = LCSData(contents)
        data.get_metadata()
    return rowcount


def select_object(key):
    key = unquote_plus(key)
    if str.endswith(key, ".gz"):
        compression = "GZIP"
    else:
        compression = "NONE"
    try:
        content = ""
        resp = s3c.select_object_content(
            Bucket=settings.OPENAQ_ETL_BUCKET,
            Key=key,
            ExpressionType="SQL",
            Expression="""
                SELECT
                *
                FROM s3object
                """,
            InputSerialization={
                "CSV": {"FieldDelimiter": ","},
                "CompressionType": compression,
            },
            OutputSerialization={"CSV": {}},
        )
        for event in resp["Payload"]:
            if "Records" in event:
                content += event["Records"]["Payload"].decode("utf-8")
    except Exception as e:
        submit_file_error(key, e)
    return content

def get_measurements(key):
    start = time()
    content = select_object(key)
    fetch_time = time() - start;

    ret = []
    start = time()
    for row in csv.reader(content.split("\n")):
        if len(row) not in [3, 5]:
            continue
        if len(row) == 5:
            try:
                lon = float(row[3])
                lat = float(row[4])
                if not (
                    lon is None
                    or lat is None
                    or lat == ""
                    or lon == ""
                    or lon == 0
                    or lat == 0
                    or lon < -180
                    or lon > 180
                    or lat < -90
                    or lat > 90
                ):
                    row[3] = lon
                    row[4] = lat
                else:
                    row[3] = None
                    row[4] = None
            except Exception:
                row[3] = None
                row[4] = None
        else:
            row.insert(3, None)
            row.insert(4, None)
        if row[0] == "" or row[0] is None:
            continue
        dt = row[2]

        try:
            dt = datetime.fromtimestamp(int(dt), timezone.utc)
        except Exception:
            try:
                dt = dateparser.parse(dt).replace(tzinfo=timezone.utc)
            except Exception:
                logger.warning(f"Exception in parsing date for {dt} {Exception}")
        row[2] = dt.isoformat()
        ret.append(row)
    logger.info("get_measurements:csv: %s; size: %s; rows: %s; fetching: %0.4f; reading: %0.4f", key, len(content)/1000, len(ret), fetch_time, time() - start)
    return ret


def submit_file_error(key, e):
    """Update the log to reflect the error and prevent a retry"""
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fetchlogs
                SET completed_datetime = clock_timestamp()
                , last_message = %s
                WHERE key = %s
                """
            ),
            (f"ERROR: {e}", key),

def to_tsv(row):
    tsv = "\t".join(map(clean_csv_value, row)) + "\n"
    return tsv
    return ""

def load_measurements_file(fetchlogs_id: int):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT key
                , init_datetime
                , loaded_datetime
                , completed_datetime
                , last_message
                FROM fetchlogs
                WHERE fetchlogs_id = %s
                LIMIT 1
                ;
                """,
                (fetchlogs_id,),
            )
            rows = cursor.fetchall()
            print(rows)
            keys = [r[0] for r in rows]
            load_measurements(keys)

def load_measurements_db(limit=250, ascending: bool = False):
    order = 'ASC' if ascending else 'DESC'
    conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
    cur = conn.cursor();
    cur.execute(
        f"""
        SELECT key
        , last_modified
        , fetchlogs_id
        FROM fetchlogs
        WHERE key~E'^lcs-etl-pipeline/measures/.*\\.csv'
        AND completed_datetime is null
        ORDER BY last_modified {order} nulls last
        LIMIT %s
        ;
        """,
        (limit,),
    )
    rows = cur.fetchall()
    keys = [r[0] for r in rows]
    conn.commit()
    cur.close()
    conn.close()
    load_measurements(keys)
    return len(keys)


def load_measurements(keys):
    logger.debug(f"loading {len(keys)} measurements")
    start_time = time()
    data = []
    new = []
    for key in keys:
        new.append({"key": key})
        newdata = get_measurements(key)
        if newdata is not None:
            data.extend(newdata)

    logger.info("load_measurements:get: %s keys; %s rows; %0.4f seconds", len(keys), len(data), time() - start_time)
    if len(data) > 0:

        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:

                cursor.execute(get_query("lcs_meas_staging.sql"))
                start = time()
                write_csv(
                    cursor, new, "keys", ["key",],
                )

                iterator = StringIteratorIO(
                    (to_tsv(line) for line in data)
                )
                cursor.copy_expert(
                    """
                    COPY meas (ingest_id, value, datetime, lon, lat)
                    FROM stdin;
                    """,
                    iterator,
                )
                mrows = cursor.rowcount
                status = cursor.statusmessage
                logger.debug(f"COPY Rows: {mrows} Status: {status}")
                cursor.execute(
                    """
                    INSERT INTO fetchlogs(
                        key,
                        loaded_datetime
                    ) SELECT key, clock_timestamp()
                    FROM keys
                    ON CONFLICT (key) DO
                    UPDATE
                        SET
                        loaded_datetime=EXCLUDED.loaded_datetime
                    ;
                    """
                )
                connection.commit()
                cursor.execute(get_query("lcs_meas_ingest.sql"))
                rows = cursor.rowcount
                logger.info("load_measurements:insert: %s rows; %0.4f seconds", mrows, time() - start)
                status = cursor.statusmessage
                logger.debug(f"INGEST Rows: {rows} Status: {status}")
                cursor.execute(
                    """
                    INSERT INTO fetchlogs(
                        key,
                        last_modified,
                        completed_datetime
                    ) SELECT *, clock_timestamp()
                    FROM keys
                    ON CONFLICT (key) DO
                    UPDATE
                        SET
                        last_modified=EXCLUDED.last_modified,
                        completed_datetime=EXCLUDED.completed_datetime
                    ;
                    """
                )
                rows = cursor.rowcount
                status = cursor.statusmessage
                logger.info("load_measurements: keys: %s; rows: %s; time: %0.4f", len(keys), mrows, time() - start_time)
                connection.commit()

                for notice in connection.notices:
                    logger.debug(notice)
