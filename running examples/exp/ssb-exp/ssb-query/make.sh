#!/bin/bash
for j in {1..5}
do
for i in {1..13}
do
	java SQLProduction ../tsssbdata/data$j/result ssb-templates/$i > q$j/$i.sql
done
done
