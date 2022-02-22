import logging
import sys
import os
import json

if 'DOTENV' not in os.environ.keys():
    os.environ['DOTENV'] = '.env.testing'

if 'AWS_PROFILE' not in os.environ.keys():
    os.environ['AWS_PROFILE'] = 'python-user'

from pandas import DataFrame
from botocore.exceptions import ClientError
from openaq_fastapi.ingest.handler import cronhandler, logger
from openaq_fastapi.settings import settings

from openaq_fastapi.ingest.lcs import (
    LCSData,
    load_metadata_db,
    load_measurements_db,
    load_measurements_file,
    load_measurements,
    get_measurements,
)


from openaq_fastapi.ingest.utils import (
    load_errors,
    select_object,
    get_object,
    get_logs_from_ids,
    get_logs_from_pattern,
    unquote_plus,
)


# load_realtime('realtime-gzipped/2022-02-04/1643994434.ndjson.gz')

# logs = get_logs_from_pattern('stations/clarity', 2)
#

# station data
# logs = get_logs_from_ids(ids=[5544399, 4874871])

# for each of them lets try and import the data
# contents = []
# for row in logs:
#     contents.append(
#         {"Key": unquote_plus(row[1]), "LastModified": row[6], "id": row[0], }
#     )

# data = LCSData(contents)
# data.get_metadata()


# measurement data
logs = get_logs_from_ids(ids=[5609404])

load_measurements(logs)
