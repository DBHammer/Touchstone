#!/bin/bash
for i in {1..13}
do
	java SQLProduction.java /home/wangqingshuai/Touchstone/old-bin/varyL/dataSSBL$1/result ssb-templates/$i > q1L$1/$i.sql
done
