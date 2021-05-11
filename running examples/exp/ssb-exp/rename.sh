#!/bin/bash
rm -rf ./tsssbdata/data1
mkdir -p ./tsssbdata/data1
for f in $1/*_0.txt; 
do
	tmp=${f##*/}
    cp -- "$f" "./tsssbdata/data1/${tmp%_0.txt}.tbl"
done

cp -- "$1/result" "./tsssbdata/data1/result"