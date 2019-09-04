#!/bin/bash

HOST=$1
PORT=$2
INDEX=$3

if [ "X$HOST" == "X" -o "X$PORT" == "X" -o  "X$INDEX" == "X" ]; then
    echo "Usage: $0 <host> <port> <index>"
    exit -1
fi

echo "Creating index..."
python NYTimes.py -s $HOST -p $PORT -i $INDEX -c

echo "Loading data..."
python ../ImportCSV.py -s $HOST -p $PORT -i $INDEX -r true  -c 2 -g 1 14 -t 11 -o "%Y-%m-%d %H:%M:%S"  -f Articles.csv 
echo "Done."
