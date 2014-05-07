#!/bin/bash

for year in {1975..2002}
do
    cd $year
    ./ncdcDataRetrieval.sh $year ../USStationsID.txt
    cd ../
done
