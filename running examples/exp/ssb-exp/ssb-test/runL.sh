#!/bin/bash
java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java Touchstonessb1L$1 ../ssb-query/q1L$1/ $2 $3
