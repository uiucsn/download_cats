#!/bin/sh

FILE=$1
TABLE=$2
HOST=$3

clickhouse-client --query "INSERT INTO ${TABLE} FORMAT Parquet" -h ${HOST} \
    --http_receive_timeout=86400 --http_send_timeout=86400 --http_connection_timeout=86400 \
  < ${FILE}
