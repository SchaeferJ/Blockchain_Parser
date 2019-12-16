#!/usr/bin/env bash

echo 'Welcome to the csv import assistant!'
echo 'This script will guide you through the import process of the Bitcoin-CSV files into Neo4j'
echo 'To handle write permissions and to make sure that Neo4j is not running during import,'
echo 'superuser rights are necessary. You will be asked for your root passwort during import.'
echo ''
echo 'Please specify the location the CSVs are saved in:'
read DATA
export DATA

echo 'Please specify your Neo4j data directory:'
read DBDIR
export DBDIR

echo 'The performace of deduplication can be improved by using multiple cores.'
echo 'Please specify the number of cores (max. 8) you want to use:'
read CORES
export CORES

echo 'Ok. Loading CSVs from ' $DATA ' and setting up Neo4j in ' $DBDIR
echo ''
echo '(1/3) Deduplicating address file...'
bash ./shell-scripts/deduplicate.sh
echo '(2/3) Initializing headers...'
bash ./shell-scripts/init_headers.sh
echo '(3/3) Import files...'
bash ./shell-scripts/bitimport.sh

