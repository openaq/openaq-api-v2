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
