#!/bin/bash
for i in {1..5}
do
    for j in {14..14}
    do
        java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java TouchstoneTPCH$i ../tpch-query/q$i/ $j $j 
        java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java TPCH$i tpch/ $j $j
    done
done
