#!/usr/local/bin/python3

"""
Converts Bitcoin binary block files into CSVs for import into Neo4j.

Requires an installation of Bitcoin Core running a full node with a **fully** indexed blockchain. NOTE: Bitcoin
daemon must NOT be running during conversion.

(c) 2019 Jochen Schäfer for Südwestrundfunk AdöR
"""

import argparse
import csv
import os
import pickle
import platform

import psutil
import rocksdb
import tqdm
from blockchain_parser.blockchain import Blockchain

# Parse command-line arguments
ap = argparse.ArgumentParser()
ap.add_argument("--startblock", help="Block to start with, defaults to 0", type=int, default=0)
ap.add_argument("--endblock", help="Block to stop at, defaults to full length of blockchain", type=int,
                default=-1)
ap.add_argument("--btcdir", help="Installation path of Bitcoin Core",
                type=str, default="")
ap.add_argument("--outdir", help="Directory to store the CSVs in. Defaults to current working directory",
                type=str, default="")
ap.add_argument("--dbdir", help="Directory for the RocksDB to reside in. Defaults to current working directory",
                type=str, default="")
args = vars(ap.parse_args())

# Initialize global constants from CLI arguments
START_BLOCK: int = args['startblock']

if args['endblock'] > 0:
    END_BLOCK: int = args['endblock']

if args['outdir'] == "":
    BASE_PATH: str = os.path.join(os.getcwd(), "csv")
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    print("No output directory specified. Saving files to " + BASE_PATH)
else:
    BASE_PATH: str = args['outdir']

if args['dbdir'] == "":
    DB_PATH: str = os.path.join(os.getcwd(), "transaction_db")
    print("No database directory specified. Initializing RocksDB in " + DB_PATH)
else:
    DB_PATH: str = args['dbdir']

if args['btcdir'] == "":
    host_os = platform.system()
    if host_os == "Linux":
        BLOCK_PATH = os.path.expanduser("~/.bitcoin")
    elif host_os == "Windows":
        BLOCK_PATH = os.path.expandvars("%APPDATA%\Bitcoin")
    elif host_os == "Darwin":
        BLOCK_PATH = os.path.expanduser("~/Library/Application Support/Bitcoin")
    else:
        raise Exception("Could not determine OS. Please manually specify the path to Bitcoin Core.")
    print("No installation path for Bitcoin Core was specified. Using default path for " + host_os + " systems: " +
          BLOCK_PATH)
else:
    BLOCK_PATH: str = args['btcdir']

BLOCK_PATH = os.path.join(BLOCK_PATH, "blocks")
INDEX_PATH = os.path.join(BLOCK_PATH, "index")

if not os.path.exists(BASE_PATH):
    os.makedirs(BASE_PATH)

# Create output files
address_file = open(os.path.join(BASE_PATH, 'addresses.csv'), 'w')
address_file_w = csv.writer(address_file)

blocks_file = open(os.path.join(BASE_PATH, 'blocks.csv'), 'w')
blocks_file_w = csv.writer(blocks_file)

transaction_file = open(os.path.join(BASE_PATH, 'transactions.csv'), 'w')
transaction_file_w = csv.writer(transaction_file)

before_file = open(os.path.join(BASE_PATH, 'before-rel.csv'), 'w')
before_file_w = csv.writer(before_file)

belongs_file = open(os.path.join(BASE_PATH, 'belongs-rel.csv'), 'w')
belongs_file_w = csv.writer(belongs_file)

receives_file = open(os.path.join(BASE_PATH, 'receives-rel.csv'), 'w')
receives_file_w = csv.writer(receives_file)

sends_file = open(os.path.join(BASE_PATH, 'sends-rel.csv'), 'w')
sends_file_w = csv.writer(sends_file)

# Add coinbase as "special" address
address_file_w.writerow(['coinbase'])

# Read installed memory to allocate as much RAM as possible to database without bricking the system.
mem = psutil.virtual_memory()
db_memory = mem.available - (4 * 1024 ** 3)
print("Found " + str(round(mem.total / 1024 ** 3, 1)) + "GB of RAM on your system, " + str(
    round(mem.available / 1024 ** 3, 1)) + \
      "GB of which are available. RocksDB will use up to" + str(round(db_memory / 1024 ** 3, 1)) + "GB for Cache.")

# Define options for RocksDB-Database
opts = rocksdb.Options()
# Create new instance if not already present
opts.create_if_missing = True
# We have A LOT of BTC-Transactions, so file open limit should be increased
opts.max_open_files = 1000000
# Increase buffer size since I/O is the bottleneck, not RAM
opts.write_buffer_size = 67108864
opts.max_write_buffer_number = 3
opts.target_file_size_base = 67108864
# Bloom filters for faster lookup
opts.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(12),
    block_cache=rocksdb.LRUCache(db_memory * 0.8),
    block_cache_compressed=rocksdb.LRUCache(db_memory * 0.2))

# Load RocksDB Database
db = rocksdb.DB(DB_PATH, opts)

# Load Blockchain
blockchain = Blockchain(BLOCK_PATH)
# Initialize iterator with respect to user specifications
if END_BLOCK < 1:
    blockchain = blockchain.get_ordered_blocks(INDEX_PATH, start=START_BLOCK)
    TOTAL_BLOCKS = len(blockchain.blockIndexes)
    print("Processing the entire blockchain.")
    print("INFO: Depending on your system, this process may take up to a week. You can interrupt the process " +
          "at any time by pressing CTRL+C.")
    iterator = blockchain
else:
    blockchain = blockchain.get_ordered_blocks(INDEX_PATH, start=START_BLOCK, end=END_BLOCK)
    iterator = tqdm.tqdm(blockchain, total=END_BLOCK)

for block in iterator:
    block_height = block.height
    block_hash = block.hash
    block_timestamp = block.header.timestamp.strftime('%Y-%m-%dT%H:%M')
    block_date = block.header.timestamp.strftime('%Y-%m-%d')
    previous_block_hash = block.header.previous_block_hash

    blocks_file_w.writerow([block_hash, block_height, block_timestamp])
    before_file_w.writerow([previous_block_hash, block_hash, 'PRECEDES'])

    for tx in block.transactions:
        tx_id = tx.txid
        outputs = []
        addresses = []
        receives = []
        inSum = 0
        outSum = 0
        for o in range(len(tx.outputs)):
            try:
                addr = tx.outputs[o].addresses[0].address
                val = tx.outputs[o].value
                outSum += val
                outputs.append([val, addr, o])
                receives.append([tx_id, val, o, addr, 'RECEIVES'])
                addresses.append([addr])
            except Exception as e:
                val = tx.outputs[o].value
                outSum += val
                outputs.append([val, 'unknown', o])
                pass
        db.put(tx_id.encode('utf-8'), pickle.dumps(outputs))
        tx_in = tx.inputs
        if not tx.is_coinbase():
            sends = []
            for i in tx_in:
                in_hash = i.transaction_hash
                in_index = i.transaction_index
                try:
                    in_transaction = pickle.loads(db.get(in_hash.encode('utf-8')))
                    in_value = in_transaction[in_index][0]
                    if len(in_transaction)==1:
                        db.delete(in_hash.encode('utf-8'))
                    inSum += in_value
                    in_address = in_transaction[in_index][1]
                    sends.append([in_address, in_value, tx_id, 'SENDS'])
                except Exception as e:
                    print(e)
                    continue
                    # raise Exception("Meh.")
                del in_transaction, in_address, in_value, in_hash, in_index
        else:
            sends = [["coinbase", sum(map(lambda x: x.value, tx.outputs)), tx_id, 'SENDS']]
            inSum = sends[0][1]

        inDegree = len(sends)
        outDegree = len(tx.outputs)

        # Write CSV files
        transaction_file_w.writerow([tx_id, block_date, inDegree, outDegree, inSum, outSum])
        belongs_file_w.writerow([tx_id, block_hash, 'BELONGS_TO'])
        address_file_w.writerows(addresses)
        receives_file_w.writerows(receives)
        sends_file_w.writerows(sends)

# Finalize
address_file.close()
blocks_file.close()
transaction_file.close()
before_file.close()
belongs_file.close()
receives_file.close()
sends_file.close()
