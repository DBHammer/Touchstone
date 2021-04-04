#!/bin/bash
for i in {1..5}
do
./load/load.sh 127.0.0.1 load/mysql/mysqlMakeDataBase$i.sql
./load/load.sh 127.0.0.1 load/touchstoneMysql/touchstoneMysqlMake$i.sql
done
./load/load.sh 127.0.0.1 load/mysql/mysqlMakeDataBase5.sql

cd /home/wangqingshuai/Touchstone/tpch-exp/tpch-test
./run.sh
