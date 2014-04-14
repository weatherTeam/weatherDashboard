#!/bin/sh

# Usage:
# The first argument is the folder name on the FTP server
#   which is normally the year
# The second argument is the name of a file listing
#   the files we want to download

### Parameters ###
year=""
filename=""

# checking the arguments
if [ $# -ne 2 ]; then
    echo "Usage : ncdcDataRetrieval.sh year filelist"
elif [ -z $1 ] || [ -z $2 ]; then
    echo "No arguments should be null"
elif [ -s $2 ] && [ -f $2 ] && [ -r $2 ]; then
    # file is not empty, is not a directory and is readable
    year="$1"
    filename="$2"
    echo "Downloading..."
    (
      echo open ftp.ncdc.noaa.gov
      echo user anonymous alpsweather@groupes.epfl.ch
      echo binary
      echo cd pub/data/noaa/$year
      while read p; do
        echo get $p
      done < $filename
      echo close
      echo bye
    ) | ftp -n > log.txt
    echo "Done."
    echo "It is possible that the connection got lost. Check log.txt"
else
    echo "Not a file, file empty or not readable"
fi
