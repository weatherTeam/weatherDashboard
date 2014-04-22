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

    # Creating temporary directory
    mkdir tmp

    # Getting the filelist of the folder we're going to download from
    ftp -n  ftp.ncdc.noaa.gov <<EOF
user anonymous alpsweather@groupes.epfl.ch
cd pub/data/noaa/$year
binary
prompt
mls *.gz tmp/ftp_list.txt
quit
EOF

    # Ordering the list
    cat tmp/ftp_list.txt | sort > tmp/ftp_list_sorted.txt

    # Intersection of the filelist of the files we want to download and the actual files on the server
    comm -12 tmp/ftp_list_sorted.txt $filename > tmp/files_to_retrieve.txt

    # Download of the files
    echo "Downloading..."
    (
      echo open ftp.ncdc.noaa.gov
      echo user anonymous alpsweather@groupes.epfl.ch
      echo binary
      echo cd pub/data/noaa/$year
      while read p; do
        echo get $p
      done < tmp/files_to_retrieve.txt
      echo close
      echo bye
    ) | ftp -n > log.txt

    # Deleting the temporary directory and its content
    rm -r tmp/

    echo "Done."
else
    echo "Not a file, file empty or not readable"
fi
