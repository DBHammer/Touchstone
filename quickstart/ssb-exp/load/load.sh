#!/bin/bash
mysql -uqswang -pBiui1227.. -h$1 --local-infile=1 < $2

