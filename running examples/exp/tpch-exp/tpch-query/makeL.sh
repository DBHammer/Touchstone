#!/bin/bash
for i in {1..16}
do
	java SQLProduction.java /home/wangqingshuai/Touchstone/old-bin/varyL/dataL$1/result qtemplates/$i > q1L$1/$i.sql
done

