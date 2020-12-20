#!/bin/sh

FILE=$1
TABLE=$2
HOST=$3

curl \
    "http://${HOST}:8123/?query=INSERT%20INTO%20${TABLE}%20FORMAT%20CSV" \
    --data-binary @${FILE} \
    -H "Transfer-Encoding: chunked"
