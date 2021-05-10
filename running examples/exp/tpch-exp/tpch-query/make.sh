#!/bin/bash
for j in {1..5}
do
for i in {1..16}
do
	java SQLProduction.java /home/wangqingshuai/Touchstone/tpch-exp/tstpchdata/data${j}/result qtemplates/$i > q$j/$i.sql
done
done
