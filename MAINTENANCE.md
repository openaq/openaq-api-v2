# Error Checking

There are two places to check for errors, the rejects table and the fetchlogs. You can refer to the openaq-db schema for details on both tables and ways to query them but generally you can do the following.

## Tools
Tools to make error checking and fixing easier

### check.py
List, summarize, evaluate and fix errors related to file ingestion. The tool has the following options:
* --env: (string) provide the name of the .env file to load. If no value is provided we default to `.env`
* --profile: (string) the name of the AWS profile to use. If no value is provided the default profile is used, if set.
* --summary: Provide a summary of errors instead of a list
* --fix: Attempt to fix the errors after listing
* --dryrun: Echo out the updated file instead of saving it back to the s3 bucket
* --debug: Debug level logging
* --id: (int) check a specific log record based on the fetchlogs_id value
* --n: (int) limit the list to n records or the summary to the past n days
* --rejects: Echo out the list of rejects. If used with --summary it will show a summary of the rejects table
* --resubmit: Update the fetchlogs table to force a reload of that id



## Ingestion errors

Whenever we catch an error during ingestion we register that error in the fetchlogs in the `last_message` field. The error should always take the format of `ERROR: message` and should be as specific as possible. Right now the predominant error is a writing error that results in a truncated JSON object that leads to a parsing error during ingestion. The current approach to fixing such an error is to remove the truncated lines and resubmit the file.

To see a summary of the last 30 days use the following
```shell
python3 check.py --summary --n 30
```

Or see a more detailed list of the last 10 errors. The list method will also download the file and check it for errors.

```shell
python3 check.py --n 10
```

Or you can check on a specific file by using the `--id` argument. This will also download the file and check it.
```shell
python3 check.py --id 5555555
```

And then if you want to try and fix the file you can use
```shell
python3 check.py --id 5555555 --fix
```

Or you can batch fix files by skipping the `--id` argument. The following will check the last 10 errors and fix them if possible.
```shell
python3 check.py --n 10 --fix
```

## Ingestion rejects
For the LCS pipeline we can have files that contain rejected values but not errors. In this case we add the rejected records into the `rejects` table for later review. The following line will display a rejects summary based on the ingest id and the file the data comes from. The ingest id is broken up into the first part, the provider, and the second part, the source id for reference.

```shell
python3 check.py --rejects --n 10
```

We also attempt to match that data to the sensor nodes table to determine if a node already exists. If the node id is returned with the rest of the data than the likely reason for the rejection is that the node did not exist at the time the measurements were being ingested but it does now. For this type of error the likely fix is to just resubmit the file.

```shell
python3 check.py --id 555555 --resubmit
```

If the node id is not returned than it is likely that it does not exist for some reason. In that scenario we need to search for the station file for that node and see if that exists. If the node does not exist the `--rejects` method will automatically check for files matching the `provider/source` pattern and return those files. If one of them looks like a good candidate you can resubmit that file.

```shell
python3 check.py --id 555554 --resubmit
```
