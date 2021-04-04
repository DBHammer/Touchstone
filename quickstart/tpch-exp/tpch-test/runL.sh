#!/bin/bash
java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java TouchstoneTPCH1L3 ../tpch-query/q1L3/ 1 16
java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java TouchstoneTPCH1L5 ../tpch-query/q1L5/ 1 16
java -classpath mysql-connector-java-8.0.23.jar CompareLatency.java TPCHRM tpch/ 1 16