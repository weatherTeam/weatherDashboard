#!/bin/bash
for i in {1977..2013}
do
    cd $i
    echo "== Processing $i =="
    echo "Gunzipping files ..."
    gunzip *.gz
    rm log.txt
    rm ncdcDataRetrieval.sh
    echo "$i uncompressed size: $(du -sh)"
    echo "Merging files ..."
    cat * > USA_$i
    echo "Gzipping file ..."
    gzip USA_$i
    echo "$i done."
    cd ../
done
