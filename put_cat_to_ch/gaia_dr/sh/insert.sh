#!/bin/bash

FILE=$1
TABLE=$2
HOST=$3

gunzip -d -c "${FILE}" |
  grep -v '^#' |                          # Remove header
  sed 's/True/1/g' | sed 's/False/0/g' |  # Convert boolean to UInt8
  sed 's/"//g' |                          # Remove quotes
  clickhouse-client \
    --query "INSERT INTO ${TABLE} FORMAT CSVWithNames" \
    --format_csv_null_representation='null' \
    --input_format_parallel_parsing=0 \
    -h ${HOST} \
    --http_receive_timeout=86400 --http_send_timeout=86400 --http_connection_timeout=86400
