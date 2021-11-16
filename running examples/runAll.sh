
#!/bin/bash
mkdir data
mkdir outerJoin
rm -f data/*
rm -f log/*
rm -f query-init/query/*

nohup java -jar RunDataGenerator.jar conf/tpch.conf 0 > log/dg1.log 2>&1 &

time java -jar RunController.jar conf/tpch.conf

chmod +x rename.sh

./rename.sh

mkdir data/query

cd query-init

chmod +x make.sh

./make.sh


