sort $DATA/addresses.csv | uniq > $DATA/addresses_dedup.csv
rm $DATA/addresses.csv
