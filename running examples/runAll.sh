
#!/bin/bash
mkdir data
mkdir outerJoin
rm -f data/*
rm -f log/*
rm -f query-init/query/*

nohup java -jar RunDataGenerator.jar conf/tpch.conf 0 > log/dg1.log 2>&1 &
nohup java -jar RunDataGenerator.jar conf/tpch.conf 1 > log/dg2.log 2>&1 &
nohup java -jar RunDataGenerator.jar conf/tpch.conf 2 > log/dg3.log 2>&1 &
nohup java -jar RunDataGenerator.jar conf/tpch.conf 3 > log/dg4.log 2>&1 &

time java -jar RunController.jar conf/tpch.conf
