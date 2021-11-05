import psycopg2
import os
import sys
import boto3
import pytz
from pathlib import Path
import json
import csv
import dateparser
import io
import gzip
import orjson
import warnings

from io import StringIO

from pydantic import BaseSettings
from datetime import datetime, timezone
from urllib.parse import unquote, unquote_plus


warnings.filterwarnings("ignore", message="The localize method is no longer necessary");

dir_path = os.path.dirname(os.path.realpath(__file__))

# get the proper keys from .env
# s3c = boto3.client(
#     's3',
#     aws_access_key_id = credentials['key'],
#     aws_secret_access_key = credentials['secret']
# )


class Settings(BaseSettings):
    DATABASE_URL: str
    DATABASE_WRITE_URL: str
    OPENAQ_ENV: str = "staging"
    OPENAQ_FASTAPI_URL: str
    TESTLOCAL: bool = True
    OPENAQ_FETCH_BUCKET: str
    OPENAQ_ETL_BUCKET: str
    OPENAQ_CACHE_TIMEOUT: int = 900


settings = Settings(
    OPENAQ_ENV = "local",
    OPENAQ_FETCH_BUCKET = "testdata"
)

def clean_csv_value(value):
    if value is None or value == "":
        return r"\N"
    return str(value).replace("\n", "\\n").replace("\t", " ")

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

def to_tsv(row):
    tsv = "\t".join(map(clean_csv_value, row)) + "\n"
    return tsv
    return ""


def get_bucket_file(key):
    """Get the file from the ETL bucket designated in settings"""
    key = unquote_plus(key)
    print(key)
    if str.endswith(key, ".gz"):
        compression = "GZIP"
    else:
        compression = "NONE"
    try:
        print(f"getting {key}")
        resp = s3c.select_object_content(
            Bucket=settings.OPENAQ_FETCH_BUCKET,
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

        return content
    except Exception as e:
        print(f"Could not find {key} {e}")

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
        print(f"Could not find {key} {e}")


def get_file(key):
    """Get file handler that determines where the file is"""
    if settings.OPENAQ_ENV == "local":
        return get_local_file(key)
    else:
        return get_bucket_file(key)


def load_measurements(key):
    key = unquote_plus(key)
    print(key)

    try:
        content = get_file(key)
        ret = []
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
            except Exception as e:
                # assume the error is because dt is not an int
                # and just attempt to parse it
                try:
                    dt = dateparser.parse(dt).replace(tzinfo=timezone.utc)
                except Exception as e:
                    print(f"Exception in parsing date for {dt} {e}")
            row[2] = dt.isoformat()
            ret.append(row)
        return ret
    except Exception as e:
        print(f"Could not load {key} {e}")
    return None


def get_query(file, **params):
    # print(f"{params}")
    query = Path(os.path.join(dir_path, file)).read_text()
    if params is not None and len(params) >= 1:
        print(f"adding parameters {params}")
        query = query.format(**params)
    return query

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
    print(f"wrote {cursor.rowcount} rows to {table}")
    # print("status:", cursor.statusmessage)

def reset_database(source_name):
    try:
        conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
        cur = conn.cursor();
        conn.set_session(autocommit=True)
        cur.execute(
            """
            WITH deletes AS (
            DELETE FROM fetchlogs
            RETURNING 1)
            SELECT COUNT(1)
            FROM deletes;
            """
        )
        row = cur.fetchone()
        print(f"Deleted {row} rows from the fetch logs")
        cur.execute(
            """
            WITH deletes AS (
            DELETE FROM rejects
            RETURNING 1)
            SELECT COUNT(1)
            FROM deletes;
            """
        )
        row = cur.fetchone()
        print(f"Deleted {row} rows from the rejects")
        cur.execute(
            """
            WITH list AS (
            SELECT s.sensors_id
            , ss.sensor_systems_id
            , n.sensor_nodes_id
            FROM sensors s
            JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
            JOIN sensor_nodes n ON (ss.sensor_nodes_id = n.sensor_nodes_id)
            WHERE n.source_name = %s
            ), s1 AS (DELETE FROM sensors WHERE sensors_id IN (SELECT sensors_id FROM list)
            ), s2 AS (DELETE FROM sensor_systems WHERE sensor_systems_id IN (SELECT sensor_systems_id FROM list)
            ) DELETE FROM sensor_nodes WHERE sensor_nodes_id IN (SELECT sensor_nodes_id FROM list);
            """,
            (source_name,),
        )
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Fetch log reset failed: {e}")


def update_database():
    try:
        conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
        cur = conn.cursor();
        conn.set_session(autocommit=True)
        cur.execute(get_query("../../../openaq-db/openaqdb/tables/versions.sql"))
        cur.close()
        conn.close()
    except Excpetion as e:
        print(f"Database update failed: {e}")

def check_sensor_rejects():
    print("----- Sensor rejects")
    try:
        conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
        cur = conn.cursor();
        cur.execute(
            """
            SELECT r->>'ingest_id' as sensor
            , COUNT(1) as n
            FROM rejects
            WHERE reason = 'SENSOR_MISSING'
            GROUP BY r->>'ingest_id';
            """
        )
        rows = cur.fetchall()
        for row in rows:
            print(f"{row[0]}: {row[1]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to check rejects: {e}")



def check_database():
    try:
        conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
        cur = conn.cursor();
        conn.set_session(autocommit=True)
        cur.execute(get_query("check_database.sql"))
        for notice in conn.notices:
            print(f"CHECK DATABASE {notice}")
        cur.close()
        conn.close()
    except Excpetion as e:
        print(f"Database update failed: {e}")


def queue_files(filetype):
    # get a list of the files
    bucket = settings.OPENAQ_FETCH_BUCKET
    dpath = os.path.join(bucket, filetype)
    files = [os.path.join(dpath, f) for f in os.listdir(dpath)]
    # fake a modified date/time
    last_modified = datetime.now().replace(
        tzinfo=timezone.utc
    )
    # add them all to the quue
    try:
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            with connection.cursor() as cursor:
                connection.set_session(autocommit=True)
                for file in files:
                    cursor.execute(
                        """
                        INSERT INTO fetchlogs (key, last_modified)
                        VALUES(%s, %s)
                        ON CONFLICT (key) DO UPDATE
                        SET last_modified=EXCLUDED.last_modified,
                        completed_datetime=NULL
                        RETURNING key;
                        """,
                        (file, last_modified,),
                    )
                    row = cursor.fetchone()

                connection.commit()
    except Exception as e:
        print(f"Failed to add files to queue: {e}")


def load_measurements_db(limit=250):
    bucket = settings.OPENAQ_FETCH_BUCKET
    try:
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            with connection.cursor() as cursor:
                connection.set_session(autocommit=True)
                cursor.execute(
                    f"""
                    SELECT key,last_modified FROM fetchlogs
                    WHERE key~E'^{bucket}/measures/.*\\.csv' AND
                    completed_datetime is null
                    ORDER BY last_modified desc nulls last
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
                keys = [r[0] for r in rows]

                # print(f"Found {len(keys)} files to ingest")
                # Start by setting up the measure staging table
                cursor.execute(get_query("lcs_meas_staging.sql"))

                data = []
                new = []
                for key in keys:
                    new.append({"key": key})
                    newdata = load_measurements(key)
                    if newdata is not None:
                        data.extend(newdata)

                # print(data)
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
                    print(f"MEASUREMENTS COPIED: {rows} Status: {status}")
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
                # print(f"INGEST Rows: {rows} Status: {status}")
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
                    print(f"LOAD MEASUREMENTS {notice}")
                print(f"UPDATED LOGS Rows: {rows} Status: {status}")


    except Exception as e:
        print(f"Failed to ingest measurements: {e}")


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
            # print(f"key: {key}")
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
            print(f"Could not load {key} {e}")


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
                    print(f"METADATA INGEST {notice}")

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
                    print(f"{key} {last_modified}")
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
                        print(f"{key} {last_modified}")
                        self.keys.append(
                            {"key": key, "last_modified": last_modified}
                        )
                        self.get_station(key)
                        hasnew = True

                if hasnew:
                    self.load_data()
                # for notice in connection.notices:
                #    print(notice)



class LocalLCSData(LCSData):
    def get_station(self, key):
        try:
            content = get_local_file(key)
            self.node(orjson.loads(content))
        except Exception as e:
            print(f"Could not load {key} {e}")


def load_metadata_db(limit=250):
    bucket = settings.OPENAQ_FETCH_BUCKET
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    SELECT key,last_modified FROM fetchlogs
                    WHERE key~*'^{bucket}/stations/' AND
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
                print(f"Load Metadata Notice: {notice}")
    if len(contents) > 0:
        data = LocalLCSData(contents)
        data.get_metadata()


def load_versions_db(limit=250):
    bucket = settings.OPENAQ_FETCH_BUCKET
    try:
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            with connection.cursor() as cursor:
                connection.set_session(autocommit=True)
                cursor.execute(
                    f"""
                    UPDATE fetchlogs
                    SET loaded_datetime = clock_timestamp()
                    WHERE key~E'^{bucket}/versions/'
                    AND completed_datetime is null
                    RETURNING key, last_modified
                    """
                )
                rows = cursor.fetchall()
                versions = []
                for row in rows:
                    raw = get_file(unquote_plus(row[0]))
                    j = orjson.loads(raw)
                    version = {}
                    metadata = {}
                    for key, value in j.items():
                        if key in ["parent_sensor_id", "sensor_id", "version_id", "life_cycle_id", "readme"]:
                            version[key] = value
                        elif key not in ["merged"]:
                            metadata[key] = value
                    version["metadata"] = orjson.dumps(metadata).decode()
                    versions.append(version)

                # create a temporary table for matching
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ms_versions (
                    sensor_id text,
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
                        "readme",
                        "metadata",
                    ],
                )
                # now process that version data as best we can

                # now add each of those to the database
                print(versions)

    except Exception as e:
        print(f"Failed to ingest versions: {e}")


# Here are the things I want to test here

# 1.
# given a set of files in the measurements, versions and stations directories
# make sure that they all get loaded into the queue
# this is a process that is currently handled by a trigger on the S3 bucket
# which is fired when the client files are processed and the files are
# put in the respective buckets.

print("---- Checking database")
check_database()

## add the version tables
# update_database();

## technically we dont really need to reset since the method is an upsert
print("----- Reseting database")
reset_database('versioning')


print("---- Queuing files")
#queue_files('measures')
#queue_files('stations')
queue_files('versions')

print("----- Loading versions")
load_versions_db()
sys.exit()

print("---- Loading metadata")
load_metadata_db()

# 2.TWE12!@lve

# make sure that ingesting all the measurement files results in the correct
# amount of sensors created. Regardless of the order of the files.
print("----- Loading measurements")
load_measurements_db()

print("----- Loading versions")
load_versions_db()


check_sensor_rejects()

# 3.
# Make sure that the version information is being ingested
