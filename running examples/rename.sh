#!/bin/bash
for f in data/*_0.txt; do
    mv -- "$f" "${f%_0.txt}.tbl"
done
