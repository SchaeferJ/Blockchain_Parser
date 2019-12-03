#!/usr/bin/env bash
sudo systemctl stop neo4j
sudo chmod -R a+rwx /var/log/neo4j/
sudo chmod -R a+rwx /ssd2/neo4j/

export DATA=/ssd/bitcoin-csv/data
export HEADERS=/ssd/bitcoin-csv/headers

neo4j-admin import \
--mode=csv \
--nodes:Address $HEADERS/addresses-header.csv,$DATA/addresses_dedup.csv \
--nodes:Block $HEADERS/blocks-header.csv,$DATA/blocks.csv \
--nodes:Transaction $HEADERS/transactions-header.csv,$DATA/transactions.csv \
--relationships:IS_BEFORE $HEADERS/before-rel-header.csv,$DATA/before-rel.csv \
--relationships:BELONGS_TO $HEADERS/belongs-rel-header.csv,$DATA/belongs-rel.csv \
--relationships:RECEIVES $HEADERS/receives-rel-header.csv,$DATA/receives-rel.csv \
--relationships:SENDS $HEADERS/sends-rel-header.csv,$DATA/sends-rel.csv \
--ignore-missing-nodes=true \
--ignore-duplicate-nodes=true \
--multiline-fields=true \
--high-io=true

sudo chmod -R a+rwx /var/log/neo4j/
sudo chmod -R a+rwx /ssd2/neo4j/
sudo systemctl start neo4j
