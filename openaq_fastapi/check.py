import argparse
import logging
import sys
import os
import json

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser();
parser.add_argument('--id', type=int, required=True);
parser.add_argument('--env', type=str, required=False);
parser.add_argument('--profile', type=str, required=False);
parser.add_argument('--fix', action="store_true");
parser.add_argument('--dryrun', action="store_true");
parser.add_argument('--debug', action="store_true");
args = parser.parse_args();

if 'DOTENV' not in os.environ.keys() and args.env is not None:
    os.environ['DOTENV'] = args.env

if 'AWS_PROFILE' not in os.environ.keys() and args.profile is not None:
    os.environ['AWS_PROFILE'] = args.profile

if args.dryrun:
    os.environ['DRYRUN'] = 'True'

if args.debug:
    os.environ['LOG_LEVEL'] = 'DEBUG'
    
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
    put_object,
    get_logs_from_ids,
    mark_success,
)

def check_realtime_key(key: str, fix: bool = False):
    """Check realtime file for common errors"""    
    logger.debug(f"\n## Checking realtime for issues: {key}")
    # get text of object
    try:
        txt = get_object(key)
    except Exception as e:
        logger.error(f"\t*** Error getting file: {e}")
        return;
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
            logger.error(f"\t*** Loading error on line #{jdx} (of {n}): {e}\n{line}")
        try:
            # then we can try to parse it
            row = parse_json(obj)
        except Exception as e:
            errors.append(jdx)
            logger.error(f"\t*** Parsing error on line #{jdx} (of {n}): {e}\n{line}")

    if len(errors)>0 and fix:
        # remove the bad rows and then replace the file
        nlines = [l for i, l in enumerate(lines) if i not in errors]
        logger.info(f"Removed {len(errors)} and now have {len(nlines)} lines")
        ntext = "\n".join(nlines)
        put_object(
            data=ntext,
            key=key
        )
        mark_success(key=key, reset=True)
    elif len(errors)==0 and fix:
        mark_success(key=key, reset=True)
        
# get the details for that id
logs = get_logs_from_ids(ids=[args.id])

## get just the keys
keys = [log[1] for log in logs]

## loop through and check each
for idx, key in enumerate(keys):
    # figure out what type of file it is
    if 'realtime' in key:
        check_realtime_key(key, args.fix)
    


            
