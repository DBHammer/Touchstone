#!/bin/bash
mysql -u$3 -p$4 -h$1 --local-infile=1 < $2
