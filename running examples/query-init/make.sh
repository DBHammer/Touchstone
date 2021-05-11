#!/bin/bash
for i in {1..16}
do
	java SQLProduction ../data/result qtemplates/$i > query/$i.sql
done
