import logging
import sys
import os
import json

if 'DOTENV' not in os.environ.keys():
    os.environ['DOTENV'] = '.env.testing'

if 'AWS_PROFILE' not in os.environ.keys():
    os.environ['AWS_PROFILE'] = 'python-user'

from botocore.exceptions import ClientError
from openaq_fastapi.ingest.handler import cronhandler, logger
from openaq_fastapi.settings import settings

from openaq_fastapi.ingest.lcs import (
    load_metadata_db,
    load_measurements_db,
    load_measurements_file,
    load_measurements,
    get_measurements,
)

from openaq_fastapi.ingest.fetch import (
    load_realtime,
    parse_json,
)

from openaq_fastapi.ingest.utils import (
    load_errors,
    select_object,
    get_object,
    get_logs_from_ids,
)


# load_realtime('realtime-gzipped/2022-02-04/1643994434.ndjson.gz')

logs = get_logs_from_ids(ids=[5634328])

# logs = load_errors()

keys = [log[1] for log in logs]

#load_realtime(keys)

print(f"Found {len(keys)} potential errors")

for idx, key in enumerate(keys):
    print(f"\n## Checking #{idx}: {key}")
    # get text of object
    try:
        txt = get_object(key)
    except Exception as e:
        print(f"\t*** Error getting file: {e}")
        continue
    # break into lines
    lines = txt.split("\n")
    # check parse for each line
    n = len(lines)
    errors = []
    for jdx, line in enumerate(lines):
        try:
            # first just try and load it
            obj = json.loads(line)
        except Exception as e:
            errors.append(jdx)
            print(f"\t*** Loading error on line #{jdx} (of {n}): {e}\n{line}")
        try:
            # then we can try to parse it
            row = parse_json(obj)
        except Exception as e:
            errors.append(jdx)
            print(f"\t*** Parsing rror on line #{jdx} (of {n}): {e}\n{line}")



# load_realtime(keys)
 # load_realtime([
 #    'realtime-gzipped/2022-02-05/1644020232.ndjson.gz',
 #    'realtime-gzipped/2022-02-05/1644068231.ndjson.gz'
 # ])

# errors = load_errors(10)

# print(f"Found {len(errors)} possible error files")

# for file in errors:
#     key = file[3]
#     print(f"Checking file {key}")
#     try:
#         obj = select_object(key)
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'JSONParsingError':
#             print("There was an error parsing the file, fetching as raw file")
#             print(e.response['Error'])
#             obj = get_object(key)
#         else:
#             print("Some other error")
#     except Exception as e:
#         print(f"post-boto error: {e}")
#         obj = get_object(key)

#     print(obj[-50:])
#     # save the file locally
#     filepath = os.path.join(settings.LOCAL_SAVE_DIRECTORY, key)
#     print(f"Writing file to {filepath}")
#     os.makedirs(os.path.dirname(filepath), exist_ok=True)
#     fle = open(filepath.replace(".gz", ""), 'w')
#     fle.write(obj)
#     fle.close()
