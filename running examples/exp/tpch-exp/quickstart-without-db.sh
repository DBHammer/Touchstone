#!/bin/bash
user=$1
passwd=$2
datadir=$3

echo "Start getting ts data..."
./rename.sh $datadir
echo "Generating query..."
cd ./tpch-query
./make.sh
echo "End!"
