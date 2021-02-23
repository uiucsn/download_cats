#!/bin/sh

TABLE=$1
HOST=$2

clickhouse-client -h $HOST --query "INSERT INTO ${TABLE} FORMAT RowBinary" <&0
