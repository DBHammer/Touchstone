#!/bin/bash
#for j in {1..5}
j=1
#do
for i in {1..16}
do
	java SQLProduction ../tstpchdata/data${j}/result qtemplates/$i > q$j/$i.sql
#done
done
