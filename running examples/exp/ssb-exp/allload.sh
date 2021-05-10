#!/bin/bash
for i in {1..5}
do 
./load/load.sh 127.0.0.1 load/ssb-mysql-touchstone/ssb$i.sql
done

cd /home/wangqingshuai/Touchstone/ssb-exp/ssb-test
./run.sh
