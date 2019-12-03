#!/usr/bin/env bash
# Creates Header-Files for node4j import
mkdir headers
echo 'txid:ID(Transaction),date:datetime{timezone:UTC},inDegree:int, outDegree:int, inSum:int, outSum:int' > ./headers/transactions-header.csv
echo 'address:ID(Address)' > ./headers/addresses-header.csv
echo ':START_ID(Block),:END_ID(Block),:TYPE' > ./headers/before-rel-header.csv
echo ':START_ID(Transaction),:END_ID(Block),:TYPE' > ./headers/belongs-rel-header.csv
echo 'hash:ID(Block),height:int,mediantime:datetime{timezone:UTC}' > ./headers/blocks-header.csv
echo ':START_ID(Transaction),value:int,output_nr:int,:END_ID(Address),:TYPE' > ./headers/receives-rel-header.csv
echo ':START_ID(Address),value:int,:END_ID(Transaction),:TYPE' > ./headers/sends-rel-header.csv
