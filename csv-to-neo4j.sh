#!/usr/bin/env bash

echo Welcome to the csv import assistant!
echo Please specify the location the CSVs are saved in:
read DATA

echo Please specify your Neo4j data directory:
read DBDIR

#bash ./shell-scripts/deduplicate.sh
#bash ./shell-scripts/init_headers.sh
#bash ./shell-scripts/bitimport.sh
echo $DATA
echo $DBDIR
