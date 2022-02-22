import argparse
import logging
import os
# import json
# from pandas import DataFrame
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--id', type=int, required=False)
parser.add_argument('--env', type=str, required=False)
parser.add_argument('--profile', type=str, required=False)
parser.add_argument('--dir', type=str, required=False)
parser.add_argument('--dryrun', action="store_true")
parser.add_argument('--debug', action="store_true")
args = parser.parse_args()

if 'DOTENV' not in os.environ.keys() and args.env is not None:
    os.environ['DOTENV'] = args.env

if 'AWS_PROFILE' not in os.environ.keys() and args.profile is not None:
    os.environ['AWS_PROFILE'] = args.profile

if args.dryrun:
    os.environ['DRYRUN'] = 'True'

if args.debug:
    os.environ['LOG_LEVEL'] = 'DEBUG'

# needs to be done AFTER the parser
from openaq_fastapi.ingest.handler import cronhandler
from openaq_fastapi.ingest.utils import (
    add_fetchlog,
)
from openaq_fastapi.settings import settings

logger = logging.getLogger(__name__)

logger.info(f'Working with {settings.DATABASE_URL}')
logger.debug(settings)

if args.dir is not None:
    logger.info(f'Adding files to fetchlogs from {args.dir}')
    path = Path(args.dir)
    # Add everything from the local directory
    for e in path.rglob('**/*'):
        if e.is_file():
            row = add_fetchlog(str(e))
            logger.debug(f'{row[0]}: {row[1]}')

cronhandler({
    "source": "manual",
    "pipeline_limit": 10,
    "realtime_limit": 10,
    "metadata_limit": 10,
    "versions_limit": 10,
}, {})
