import os
import logging
from datetime import datetime, timezone
import dateparser
import pytz
import orjson
import csv
from urllib.parse import unquote, unquote_plus

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
logger.setLevel("DEBUG");


class LCSData:
    def __init__(
            self, page=None, key=None, st=datetime.now().replace(tzinfo=pytz.UTC)
    ):
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
            elif key in ["site_name", "source_name", "ismobile", "country", "city"]:
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
        if str.endswith(key, ".gz"):
            compression = "GZIP"
        else:
            compression = "NONE"
        try:
            # logger.debug(f"key: {key}")
            resp = s3c.select_object_content(
                Bucket=settings.OPENAQ_FETCH_BUCKET,
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

        except Exception as e:
            logger.debug(f"Could not load {key} {e}")


    def load_data(self):
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:
                self.create_staging_table(cursor)
                write_csv(
                    cursor, self.keys, "keys", ["key", "last_modified",],
                )
                cursor.execute(
                    """
                    INSERT INTO fetchlogs(
                        key,
                        last_modified,
                        loaded_datetime
                    ) SELECT *, clock_timestamp()
                    FROM keys
                    ON CONFLICT (key) DO
                    UPDATE
                        SET
                        last_modified=EXCLUDED.last_modified,
                        loaded_datetime=EXCLUDED.completed_datetime
                    ;;
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
                        "city",
                        "country",
                        "ismobile",
                        "geom",
                        "metadata",
                    ],
                )
                write_csv(
                    cursor,
                    self.systems,
                    "ms_sensorsystems",
                    [
                        "ingest_id",
                        "ingest_sensor_nodes_id",
                        "metadata",
                    ],
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
                connection.commit()

                for notice in connection.notices:
                    logger.debug(f"METADATA INGEST {notice}")

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
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:

                for obj in self.page:

                    key = obj["Key"]
                    last_modified = obj["LastModified"]
                    logger.debug(f"{key} {last_modified}")
                    # if last_modified > self.st:
                    cursor.execute(
                        """
                        SELECT 1 FROM fetchlogs
                        WHERE key=%s
                        AND completed_datetime IS NOT NULL
                        """,
                        (key,),
                    )
                    rows = cursor.rowcount
                    if rows < 1:
                        logger.debug(f"{key} {last_modified}")
                        self.keys.append(
                            {"key": key, "last_modified": last_modified}
                        )
                        self.get_station(key)
                        hasnew = True

                if hasnew:
                    self.load_data()
                for notice in connection.notices:
                    logger.debug(notice)


def get_bucket_file(key):
    """Get the file from the ETL bucket designated in settings"""
    key = unquote_plus(key)

    if str.endswith(key, ".gz"):
        compression = "GZIP"
    else:
        compression = "NONE"

    if "json" in key:
        inputSerialization = {
            "JSON": {"Type": "Document"},
            "CompressionType": compression,
        }
        outputSerialization = {
            "JSON": {},
        }
    else:
        inputSerialization = {
            "CSV": {"FieldDelimiter": ","},
            "CompressionType": compression,
        }
        outputSerialization = {
            "CSV": {},
        }

    try:
        logger.debug(f"getting {key}")
        resp = s3c.select_object_content(
            Bucket=settings.OPENAQ_FETCH_BUCKET,
            Key=key,
            ExpressionType="SQL",
            Expression="SELECT * FROM s3object",
            InputSerialization = inputSerialization,
            OutputSerialization = outputSerialization,
        )
        content = ""
        for event in resp["Payload"]:
            if "Records" in event:
                content += event["Records"]["Payload"].decode("utf-8")
        return content
    except Exception as e:
        logger.debug(f"Could not read {key} {e}")

def get_local_file(key):
    """Get the file from the local test data directory"""
    key = unquote_plus(key)
    try:
        filepath = os.path.join(dir_path, key);
        content = ""
        if str.endswith(key, ".gz"):
            with gzip.open(filepath,'rt') as f:
                for line in f:
                    content += line;
        else:
            content = Path(filepath).read_text()
        return content
    except Exception as e:
        logger.debug(f"Could not find {key} {e}")

def get_file(key):
    """Get file handler that determines where the file is"""
    logger.debug(f"get_file: {settings.OPENAQ_ENV}/{key}")
    if settings.OPENAQ_ENV == "local":
        return get_local_file(key)
    else:
        return get_bucket_file(key)

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
    logger.debug("rowcount:", cursor.rowcount)
    logger.debug("status:", cursor.statusmessage)


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

@app.command()
def load_metadata_db(limit=250):
    bucket = settings.OPENAQ_FETCH_BUCKET
    logger.debug(f"Checking for metadata files in {settings.OPENAQ_FETCH_BUCKET}")
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    SELECT key,last_modified FROM fetchlogs
                    WHERE key~*'stations/.*\\.json' AND
                    completed_datetime is null order by
                    last_modified asc nulls last
                    limit %s;
                    """,
                (limit,),
            )
            rows = cursor.fetchall()
            contents = []
            for row in rows:
                contents.append(
                    {"Key": unquote_plus(row[0]), "LastModified": row[1],}
                )
            for notice in connection.notices:
                logger.debug(f"Load Metadata Notice: {notice}")
    if len(contents) > 0:
        data = LCSData(contents)
        data.get_metadata()

def load_versions_db(limit=250):
    bucket = settings.OPENAQ_FETCH_BUCKET
    logger.debug(f"Checking for version files in {bucket}")
    try:
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            with connection.cursor() as cursor:
                connection.set_session(autocommit=True)
                cursor.execute(
                    f"""
                    UPDATE fetchlogs
                    SET loaded_datetime = clock_timestamp()
                    WHERE key~*'versions/.*\\.json'
                    AND completed_datetime is null
                    RETURNING key, last_modified
                    """
                )
                rows = cursor.fetchall()
                versions = []
                for row in rows:
                    logger.debug(f"{row}")
                    raw = get_file(unquote_plus(row[0]))
                    j = orjson.loads(raw)
                    version = {}
                    metadata = {}
                    for key, value in j.items():
                        if key in ["parent_sensor_id", "sensor_id", "parameter", "version_id", "life_cycle_id", "readme"]:
                            version[key] = value
                        elif key not in ["merged"]:
                            metadata[key] = value
                    version["metadata"] = orjson.dumps(metadata).decode()
                    versions.append(version)

                # create a temporary table for matching
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ms_versions (
                    sensor_id text UNIQUE,
                    parent_sensor_id text,
                    life_cycle_id text,
                    version_id text,
                    parameter text,
                    readme text,
                    sensors_id int,
                    parent_sensors_id int,
                    life_cycles_id int,
                    measurands_id int,
                    metadata jsonb
                    );
                    DELETE FROM ms_versions;
                    """
                )
                # add the version data into that table
                write_csv(
                    cursor,
                    versions,
                    "ms_versions",
                    [
                        "sensor_id",
                        "parent_sensor_id",
                        "version_id",
                        "life_cycle_id",
                        "parameter",
                        "readme",
                        "metadata",
                    ],
                )
                # now process that version data as best we can
                cursor.execute(get_query("lcs_ingest_versions.sql"))
                # now add each of those to the database
                logger.debug(len(versions))
                for notice in connection.notices:
                   logger.debug(notice)


    except Exception as e:
        logger.debug(f"Failed to ingest versions: {e}")


def load_measurements(key):
    key = unquote_plus(key)
    logger.debug(key)

    if str.endswith(key, ".gz"):
        compression = "GZIP"
    else:
        compression = "NONE"
    try:
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
        content = ""
        for event in resp["Payload"]:
            if "Records" in event:
                content += event["Records"]["Payload"].decode("utf-8")

        ret = []
        logger.debug(resp)
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
                    logger.debug(f"Exception in parsing date for {dt} {Exception}")
            row[2] = dt.isoformat()
            ret.append(row)
        return ret
    except Exception as e:
        logger.debug(f"Could not load {key} {e}")
    return None


def to_tsv(row):
    tsv = "\t".join(map(clean_csv_value, row)) + "\n"
    return tsv
    return ""

@app.command()
def load_measurements_db(limit=250):
    bucket = settings.OPENAQ_FETCH_BUCKET
    logger.debug(f"Checking for measurement files in {bucket}")
    try:
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            with connection.cursor() as cursor:
                connection.set_session(autocommit=True)
                cursor.execute(
                    f"""
                    SELECT key,last_modified FROM fetchlogs
                    WHERE key~E'measures/.*\\.csv' AND
                    completed_datetime is null
                    ORDER BY last_modified desc nulls last
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
                keys = [r[0] for r in rows]

                logger.debug(f"Found {len(keys)} files to ingest")
                # Start by setting up the measure staging table
                cursor.execute(get_query("lcs_meas_staging.sql"))

                data = []
                new = []
                for key in keys:
                    new.append({"key": key})
                    newdata = load_measurements(key)
                    if newdata is not None:
                        data.extend(newdata)

                # logger.debug(data)
                if len(data) > 0:
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
                    rows = cursor.rowcount
                    status = cursor.statusmessage
                    logger.debug(f"MEASUREMENTS COPIED: {rows} Status: {status}")
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
                status = cursor.statusmessage
                # logger.debug(f"INGEST Rows: {rows} Status: {status}")
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
                connection.commit()
                for notice in connection.notices:
                    logger.debug(f"LOAD MEASUREMENTS {notice}")
                logger.debug(f"UPDATED LOGS Rows: {rows} Status: {status}")

    except Exception as e:
        logger.debug(f"Failed to ingest measurements: {e}")


if __name__ == "__main__":
    app()
