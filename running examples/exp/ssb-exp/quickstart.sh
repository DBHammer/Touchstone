#!/bin/bash
user=$1
passwd=$2
datadir=$3

echo "Start getting ts data..."
./rename.sh $datadir
echo "Start loading..."
./allload.sh $user $passwd
echo "Generating query..."
cd ./ssb-query
./make.sh
echo "End!"
