import argparse
import logging
import os
import json

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="""
    Do some basic checking against the database.
    Requires an env file with the basic database variables,
    the same that you would need to deploy.
    """)
parser.add_argument('--id', type=int, required=False,
                    help='The fetchlogs_id value')
parser.add_argument('--env', type=str, required=False,
                    help='The dot env file to use')
parser.add_argument('--profile', type=str, required=False,
                    help='The AWS profile to use')
parser.add_argument('--n', type=int, required=False, default=30,
                    help="""Either the number of entries to list
                    (sorted by date) or the number of days to go
                    back if using the summary or rejects arguments""")
parser.add_argument('--fix', action="store_true",
                    help='Automatically attempt to fix the problem')
parser.add_argument('--load', action="store_true",
                    help='Attempt to load the file manually, outside the queue')
parser.add_argument('--dryrun', action="store_true",
                    help='Check to see if its fixable but dont actually save it')
parser.add_argument('--debug', action="store_true",
                    help='Output at DEBUG level')
parser.add_argument('--summary', action="store_true",
                    help='Summarize the fetchlog errors by type')
parser.add_argument('--rejects', action="store_true",
                    help='Show summary of the rejects errors')
parser.add_argument('--resubmit', action="store_true",
                    help='Mark the fetchlogs file for resubmittal')
args = parser.parse_args()

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
    load_rejects_summary,
    get_object,
    put_object,
    get_logs_from_ids,
    get_logs_from_pattern,
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
            parse_json(obj)
        except Exception as e:
            errors.append(jdx)
            print(f"*** Parsing error on line #{jdx} (of {n}): {e}\n{line}")

    if len(errors) > 0 and fix:
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
    elif len(errors) == 0 and fix:
        mark_success(key=key, reset=True)


# If we have passed an id than we check that
if args.id is not None:
    # get the details for that id
    logs = get_logs_from_ids(ids=[args.id])
    # get just the keys
    keys = [log[1] for log in logs]
    # loop through and check each
    for idx, key in enumerate(keys):
        # if we are resubmiting we dont care
        # what type of file it is
        if args.resubmit:
            mark_success(key, reset=True, message='resubmitting')
        # figure out what type of file it is
        elif 'realtime' in key:
            if args.load:
                load_realtime([key])
            else:
                check_realtime_key(key, args.fix)
        else:
            print(key)
# Otherwise if we set the summary flag return a daily summary of errors
elif args.summary:
    rows = load_errors_summary(args.n)
    print("Type\t\tDay\t\tCount\tMin\t\tMax\t\tID")
    for row in rows:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}")
elif args.rejects:
    rows = load_rejects_summary(args.n)
    print("Provider\tSource\tLog\tNode\tRecords")
    for row in rows:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")
        if row[3] is None:
            # check for a station file
            station_keys = get_logs_from_pattern(f"{row[0]}/{row[1]}")
            for station in station_keys:
                print(f"station key: {station[1]}; log: {station[0]}")
# otherwise fetch a list of errors
else:
    errors = load_errors_list(args.n)
    for error in errors:
        print(f"------------------\nDATE: {error[2]}\nKEY: {error[1]}\nID:{error[0]}\nERROR:{error[5]}")
        if 'realtime' in error[1]:
            check_realtime_key(error[1], args.fix)
