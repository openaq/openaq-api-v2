#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

openaqfetch load-fetch-file $DIR/test.ndjson.gz
/workspace/openaq-db/run_updates.sh 2020-12-31 2020-01-01
/workspace/openaq-db/nightly_updates.sh