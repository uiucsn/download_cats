#!/bin/bash

FILE=$1
TABLE=$2
HOST=$3

gunzip -d -c "${FILE}" |
  clickhouse-client \
    --query "INSERT INTO ${TABLE} FORMAT CSV" \
    --input_format_parallel_parsing=0 \
    -h ${HOST} \
    --http_receive_timeout=86400 --http_send_timeout=86400 --http_connection_timeout=86400
