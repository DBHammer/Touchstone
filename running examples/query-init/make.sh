#!/bin/bash
for i in {1..16}
do
	java SQLProduction.java ../data/result qtemplates/$i > ../data/query/$i.sql
done
