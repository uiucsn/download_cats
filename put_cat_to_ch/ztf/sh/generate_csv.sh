#!/bin/sh

INPUT_FILE=$1
OUTPUT_FILE=$2

tar -xOzf ${INPUT_FILE} \
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
  > ${OUTPUT_FILE}
