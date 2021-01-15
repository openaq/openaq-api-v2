import boto3
import psycopg2
from ..settings import settings
from .lcs import load_measurements_db, load_metadata_db
from .fetch import load_db

from datetime import datetime, timezone

s3c = boto3.client("s3")


def handler(event, context):
    print(event)
    records = event.get("Records")
    if records is not None:
        try:
            for record in records:
                bucket = record["s3"]["bucket"]["name"]
                key = record["s3"]["object"]["key"]
                print(f"{bucket} {object}")
                lov2 = s3c.list_objects_v2(
                    Bucket=bucket, Prefix=key, MaxKeys=1
                )
                try:
                    last_modified = lov2["Contents"][0]["LastModified"]
                except KeyError:
                    print("could not get last modified time from obj")
                last_modified = datetime.now().replace(tzinfo=timezone.utc)
                with psycopg2.connect(
                    settings.DATABASE_WRITE_URL
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO fetchlogs (key, last_modified)
                            VALUES(%s, %s)
                            ON CONFLICT (key) DO UPDATE
                            SET last_modified=EXCLUDED.last_modified,
                            completed_datetime=NULL RETURNING *;
                            """,
                            (
                                key,
                                last_modified,
                            ),
                        )
                        row = cursor.fetchone()
                        print(f"{row}")
        except Exception as e:
            print(f"Exception: {e}")
    elif event.get("source") and event["source"] == "aws.events":
        print('running cron job')
        cronhandler(event, context)


def cronhandler(event, context):
    print(event)
    load_metadata_db()
    print("metadata loaded")
    load_measurements_db(250)
    print("etl data loaded")
    load_db(50)
    print("fetch data loaded")
