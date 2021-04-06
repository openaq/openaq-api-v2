#!/bin/bash
curl -L https://github.com/openaq/openaq-db/archive/refs/heads/main.zip >/openaqdb/openaqdb.zip
unzip /openaqdb/openaqdb.zip -d /openaqdb/
cd /openaqdb/openaq-db-main/openaqdb
# unset $PGHOST
# psql -c "create database aardvark;"
./init.sh
