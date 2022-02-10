import logging
import sys
from pandas import DataFrame

from openaq_fastapi.ingest.handler import cronhandler, logger

# console_log_output = sys.stdout
# console_handler = logging.StreamHandler(console_log_output)
# logger.addHandler(console_handler)

from openaq_fastapi.ingest.lcs import (
    load_metadata_db,
    load_measurements_db,
    load_measurements_file,
    get_measurements,
)


keys = [
   # 'lcs-etl-pipeline/measures/purpleair/1641551627-iipjk.csv.gz',
   # 'lcs-etl-pipeline/measures/habitatmap/1641551627-v52y.csv.gz',
  #  'lcs-etl-pipeline/measures/habitatmap/1641551687-lyk94.csv.gz',
    ]

for key in keys:
    data = get_measurements(key)
    df = DataFrame(data)
    print(df)

#load_metadata_db(2)
#load_measurements_db(1, True)
#load_measurements_file(5117654)

cronhandler({
    "source": "manual",
    "pipeline_limit": 2,
    "realtime_limit": 0,
    "metadata_limit": 0,
}, {})
