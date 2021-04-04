#!/bin/bash
mysql -uqswang -p$1 -h$2 --local-infile=1 < $3