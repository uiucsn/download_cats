#!/bin/sh

FILE=$1
TABLE=$2
HOST=$3

tar -xOzf ${FILE} \
  | awk '
    BEGIN{
      OFS=","
    }
    {
      $1=$1
    }
    {
      if ($1 == "#") {
        meta = substr($0,3)
      } else {
        print meta,$0
      }
    }' \
  | clickhouse-client --query "INSERT INTO ${TABLE} FORMAT CSV" -h ${HOST}
