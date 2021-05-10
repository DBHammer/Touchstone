#!/bin/bash
rm -rf ./tstpchdata/data1
mkdir -p ./tstpchdata/data1
for f in $1/*_0.txt; 
do
	tmp=${f##*/}
    cp -- "$f" "./tstpchdata/data1/${tmp%_0.txt}.tbl"
done

cp -- "$1/result" "./tstpchdata/data1/result"