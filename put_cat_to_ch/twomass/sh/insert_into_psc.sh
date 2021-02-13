#!/bin/bash

DIR=$1
TABLE=$2
HOST=$3

find "${DIR}" -maxdepth 1 -name 'psc_*.gz' -print0 |
  while IFS= read -r -d '' FILE; do
    gunzip -d -c "${FILE}" |
      clickhouse-client \
        --query "INSERT INTO ${TABLE} FORMAT CSV" \
        --format_csv_delimiter '|' \
        --input_format_parallel_parsing=0 \
        -h ${HOST} \
        --http_receive_timeout=86400 --http_send_timeout=86400 --http_connection_timeout=86400
  done
