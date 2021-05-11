#!/bin/bash
for i in {3..5}
do
    for j in {6..6}
    do
        java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java Touchstonessb$i ../ssb-query/q$i/ $j $j 
        java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java ssb$i ssb/ $j $j
    done
done
