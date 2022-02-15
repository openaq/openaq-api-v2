# Error Checking

There are two places to check for errors, the rejects table and the fetchlogs. You can refer to the openaq-db schema for details on both tables and ways to query them but generally you can do the following.

## Ingestion errors

Whenever we catch an error during ingestion we register that error in the fetchlogs in the `last_message` field. The error should always take the format of `ERROR: message` and should be as specific as possible. To get an overview of the current errors you can run the following query, which will tell you the number of file errors in the last 30 days. 

```sql
WITH logs AS (
SELECT init_datetime
, CASE
  WHEN key~E'^realtime' THEN 'realtime'
  WHEN key~E'^lcs-etl-pipeline/measures' THEN 'pipeline'
  WHEN key~E'^lcs-etl-pipeline/station' THEN 'metadata'
  ELSE key
  END AS type
, fetchlogs_id
FROM fetchlogs
WHERE last_message~*'^error'
AND init_datetime > current_date - 30)
SELECT type
, init_datetime::date as day
, COUNT(1) as n
, AGE(now(), MAX(init_datetime)) as min_age
, AGE(now(), MIN(init_datetime)) as max_age
, MIN(fetchlogs_id) as fetchlogs_id
FROM logs
GROUP BY init_datetime::date, type
ORDER BY init_datetime::date;
```

This gives you at least one fetchlogs_id so that you can look at a specific file error with the following

```sql
SELECT *
FROM fetchlogs
WHERE fetchlogs_id = :fetchlogs_id
```


## Fixing errors
Start out by getting a list of the errors. This could be done in a summary or a long list.

```python
# Get the last 10 errors printed out as a list
python3 openaq_fastapi/check.py --profile openaq-user --n 10
# Or get the last 10 days of errors summarized by day
python3 openaq_fastapi/check.py --profile openaq-user --n 10 --summary
```

An example of an error entry from the list method
```
KEY: realtime-gzipped/2022-02-10/1644524233.ndjson.gz
ID:5624476
ERROR:ERROR: COPY from stdin failed: error in .read() call: JSONDecodeError Expecting value: line 1 column 16 (char 15)
CONTEXT:  COPY tempfetchdata, line 22671
```


```python
python3 openaq_fastapi/check.py --profile openaq-user --id 5634328 --fix
```
