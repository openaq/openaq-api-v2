import argparse
import logging
import sys
import os
import json

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser();
parser.add_argument('--id', type=int, required=False)
parser.add_argument('--env', type=str, required=False)
parser.add_argument('--profile', type=str, required=False)
parser.add_argument('--n', type=int, required=False, default=30)
parser.add_argument('--fix', action="store_true")
parser.add_argument('--dryrun', action="store_true")
parser.add_argument('--debug', action="store_true")
parser.add_argument('--summary', action="store_true")
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
    load_errors_list,
    load_errors_summary,
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
        # these errors are not fixable so return
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
            print(f"*** Loading error on line #{jdx} (of {n}): {e}\n{line}")
        try:
            # then we can try to parse it
            row = parse_json(obj)
        except Exception as e:
            errors.append(jdx)
            print(f"*** Parsing error on line #{jdx} (of {n}): {e}\n{line}")

    if len(errors)>0 and fix:
        # remove the bad rows and then replace the file
        nlines = [l for i, l in enumerate(lines) if i not in errors]
        message = f"Fixed: removed {len(errors)} and now have {len(nlines)} lines"
        print(message)
        ntext = "\n".join(nlines)
        put_object(
            data=ntext,
            key=key
        )
        mark_success(key=key, reset=True, message=message)
    elif len(errors)==0 and fix:
        mark_success(key=key, reset=True)

# If we have passed an id than we check taht
if args.id is not None:
    # get the details for that id
    logs = get_logs_from_ids(ids=[args.id])

    ## get just the keys
    keys = [log[1] for log in logs]

    ## loop through and check each
    for idx, key in enumerate(keys):
        # figure out what type of file it is
        if 'realtime' in key:
            check_realtime_key(key, args.fix)
# Otherwise if we set the summary flag return a daily summary of errors
elif args.summary:
    rows = load_errors_summary(args.n)
    print("Type\t\tDay\t\tCount\tMin\t\tMax\t\tID")
    for row in rows:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}")
# otherwise fetch a list of errors
else:
    errors = load_errors_list(args.n)
    for error in errors:
        print(f"------------------\nDATE: {error[2]}\nKEY: {error[1]}\nID:{error[0]}\nERROR:{error[5]}")
        if 'realtime' in error[1]:
            check_realtime_key(error[1], args.fix)
