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
import math
import platform

import psutil
import rocksdb
import tqdm
from blockchain_parser.blockchain import Blockchain
from joblib import Parallel, delayed
from joblib import parallel_backend

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
ap.add_argument("--cores", help="Number of cores the parser is allowed to use",
                type=str, default="-1")
ap.add_argument("--mem", help="Maximum memory (in MB) the parser is allowed to use",
                type=str, default="-1")

args = vars(ap.parse_args())

# Check that script is running on Linux, as multiprocessing support relies on POSIX system calls
if platform.system() != "Linux":
    import sys

    sys.exit("FATAL ERROR: Parallel processing is currently only supported by Linux Systems.")

# Initialize global constants from CLI arguments
START_BLOCK: int = args['startblock']

if args['endblock'] > 0:
    END_BLOCK: int = args['endblock']

if args['outdir'] == "":
    # If no output directory is specified, save processed data to "csv" folder in current directory
    BASE_PATH: str = os.path.join(os.getcwd(), "csv")
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    print("No output directory specified. Saving files to " + BASE_PATH)
else:
    BASE_PATH: str = args['outdir']

if args['dbdir'] == "":
    # If no output directory is specified, save database to "transaction_db" folder in current directory
    DB_PATH: str = os.path.join(os.getcwd(), "transaction_db")
    print("No database directory specified. Initializing RocksDB in " + DB_PATH)
else:
    DB_PATH: str = args['dbdir']

# Set Bitcoin path to system defaults unless specified otherwise.
# See: https://en.bitcoin.it/wiki/Data_directory

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

# Create subpaths for Index and Blocks
BLOCK_PATH = os.path.join(BLOCK_PATH, "blocks")
INDEX_PATH = os.path.join(BLOCK_PATH, "index")

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

# Add coinbase as "special" address, since it does not explicitly appear in any transaction
address_file_w.writerow(['coinbase'])

# Read installed memory to allocate as much RAM as possible to database without bricking the system.
mem = psutil.virtual_memory()
# Read CPU core count to avoid oversubscription of cores
cpus = psutil.cpu_count()

# Check for user-defined memory constraints and make sure that user did not specify more RAM than installed
if 0 < args["mem"] <= mem:
    db_memory = args["mem"]
else:
    db_memory = mem.available - (4 * 1024 ** 3)

print("Found " + str(round(mem.total / 1024 ** 3, 1)) + "GB of RAM on your system, " + str(
    round(mem.available / 1024 ** 3, 1)) + \
      "GB of which are available. RocksDB will use up to" + str(round(db_memory / 1024 ** 3, 1)) + "GB for Cache.")

# Check for user-defined core constraints and make sure that user did not specify more cores than installed
if 0 < args["cores"] <= cpus:
    max_jobs = args["cores"]
else:
    max_jobs = cpus - 1

print("Found " + str(cpus) + " CPU cores on your system. " + str(max_jobs) + " cores will be used.")

# Define options for RocksDB-Database
# Optimized for fast inserts as suggested by https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
opts = rocksdb.Options()
# Create new instance if not already present
opts.create_if_missing = True
# We have A LOT of BTC-Transactions, so file open limit should be increased (-1 = infinite)
opts.max_open_files = -1
# Use Vector Memtables for faster inserts (which do not allow for concurrent writes)
opts.memtable_factory = rocksdb.VectorMemtableFactory()
opts.allow_concurrent_memtable_write = False
# Increase buffer size since I/O is the bottleneck, not RAM
opts.write_buffer_size = db_memory * 0.3
opts.max_write_buffer_number = 10
# Increase File size: Sequential reads are much faster than random reads
opts.target_file_size_base = 128 * 1024 ** 2
# Disable auto compactions because they are terribly slow. Will compact manually lateron.
opts.disable_auto_compactions = True
opts.max_background_compactions = 10
# Bulkload Options as suggested by RocksDB FAQ
opts.max_background_flushes = 15
opts.level0_file_num_compaction_trigger = -1
opts.level0_slowdown_writes_trigger = -1
opts.level0_stop_writes_trigger = 999999
opts.compression = rocksdb.CompressionType.no_compression

# Bloom filters for faster lookup
opts.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(db_memory * 0.4),
    block_cache_compressed=rocksdb.LRUCache(db_memory * 0.3))

# Load RocksDB Database
db = rocksdb.DB(DB_PATH, opts)


# Define Functions for parallel processing

def process_chunk(BLOCK_PATH, INDEX_PATH, start):
    """
    Processes a chunk of Bitcoin blocks (start to start+1000) and returns the transaction outputs

    :param BLOCK_PATH:  str, the path to the Bitcoin blocks
    :param INDEX_PATH:  str, the path to the LevelDB Bitcoin index
    :param start:       int, the block height to start at
    :return:            list, a list of tuples. One tuple per transaction, where each tuple contains the transaction id
                        and a serialized representation of a list of transaction outputs as bytestring.
    """
    re_data = []
    # Load Blockchain, ignore Read Locks imposed by other instances of the process
    blockchain = Blockchain(BLOCK_PATH, ignoreLocks=True)
    blockchain = blockchain.get_ordered_blocks(INDEX_PATH, start=start, end=start + 1000)
    for block in blockchain:
        for tx in block.transactions:
            tx_id = tx.txid
            # Create a list of outputs, where each output is itself a list comprising value, receiving address and
            # output number.
            outputs = []
            for o in range(len(tx.outputs)):
                try:
                    addr = tx.outputs[o].addresses[0].address
                    val = tx.outputs[o].value
                    outputs.append([val, addr, o])
                except Exception as e:
                    val = tx.outputs[o].value
                    outputs.append([val, 'unknown', o])
                    pass
            # Add the output list of the transaction and append it to the collector list. Serialization for the
            # the database is performed here because it is costly and should be done in paralllel.
            re_data.append((tx_id, pickle.dumps(outputs)))

    return re_data


def generate_csv(BLOCK_PATH, INDEX_PATH, start):
    """
    Processes a chunk of Bitcoin blocks and returns the values that will be written into the csv files

    :param BLOCK_PATH:  str, the path to the Bitcoin blocks
    :param INDEX_PATH:  str, the path to the LevelDB Bitcoin index
    :param start:       int, the block height to start at
    :return:            tuple, a tuple of lists. Each entry in the list corresponds to one row in the csv
    """

    # Connect to Transaction Output Database. No weird hacks requires as RocksDB natively supports concurrent reads.
    opts = rocksdb.Options()
    db = rocksdb.DB(DB_PATH, opts, read_only=True)

    # Load Blockchain, ignore Read Locks imposed by other instances of the process
    blockchain = Blockchain(BLOCK_PATH, ignoreLocks=True)
    blockchain = blockchain.get_ordered_blocks(INDEX_PATH, start=start, end=start + 1000)

    # Create output lists
    address_data = []
    blocks_data = []
    transaction_data = []
    before_data = []
    belongs_data = []
    receives_data = []
    sends_data = []

    for block in blockchain:
        # Get Block parameters
        block_height = block.height
        block_hash = block.hash
        block_timestamp = block.header.timestamp.strftime('%Y-%m-%dT%H:%M')
        block_date = block.header.timestamp.strftime('%Y-%m-%d')
        previous_block_hash = block.header.previous_block_hash

        # Append block data to lists. Note: List of lists, as the csv writer will interpret each list
        # as a new row in the file.
        blocks_data.append([block_hash, block_height, block_timestamp])
        before_data.append([previous_block_hash, block_hash, 'PRECEDES'])
        for tx in block.transactions:
            tx_id = tx.txid
            # Initialize summing variables
            inSum = 0
            outSum = 0
            for o in range(len(tx.outputs)):
                try:
                    addr = tx.outputs[o].addresses[0].address
                    val = tx.outputs[o].value
                    outSum += val
                    receives_data.append([tx_id, val, o, addr, 'RECEIVES'])
                    address_data.append([addr])
                # Some transactions contain irregular outputs (Spam, Attacks on Bitcoin,...). These will be ignored.
                except Exception as e:
                    val = tx.outputs[o].value
                    outSum += val
                    pass
            tx_in = tx.inputs
            # Coinbase transactions (newly generated coins) have no sending address. So there's no need to look it up.
            if not tx.is_coinbase():
                # Iterate over all transaction inputs
                for i in tx_in:
                    # Get hash of the transaction the coins have been last spent in
                    in_hash = i.transaction_hash
                    # Get the index of the transaction output the coins have been last spent in
                    in_index = i.transaction_index
                    try:
                        # Retrieve last spending transaction from database
                        in_transaction = pickle.loads(db.get(in_hash.encode()))
                        # Get value and receiving address of last transaction (i.e. spending address in this tx)
                        in_value = in_transaction[in_index][0]
                        in_address = in_transaction[in_index][1]
                        # Append data to return list
                        sends_data.append([in_address, in_value, tx_id, 'SENDS'])
                        inSum += in_value
                    # Catch exceptions that might occur when dealing with certain kinds of ominous transactions.
                    # This is very rare and should not break everything.
                    except Exception as e:
                        print(e)
                        continue
                    del in_transaction, in_address, in_value, in_hash, in_index
            else:
                # Simplified parsing for coinbase transactions
                sends = [["coinbase", sum(map(lambda x: x.value, tx.outputs)), tx_id, 'SENDS']]
                inSum = sends[0][1]

            # In-Degree is length of sending adddresses, out-degree the number of tx outputs
            inDegree = len(sends)
            outDegree = len(tx.outputs)

            transaction_data.append([tx_id, block_date, inDegree, outDegree, inSum, outSum])
            belongs_data.append([tx_id, block_hash, 'BELONGS_TO'])

    # Return Lists

    return (address_data, blocks_data, transaction_data, before_data, belongs_data, receives_data, sends_data)


# Create the chunks for processing. Will generate chunks of 1,000 blocks and split these chunks into equal-sized
# lists, where the number of elements corresponds to the number of parallel jobs. Thus, 100% of allocated CPU power
# can be used. Splitting processing up in several steps is necessary, as RocksDB does not allow concurrent writes.
# Writing one large batch of data after all blocks have been processed would cause the program to run out of memory.

n = max_jobs
chunks = list(range(0, 606590, 1000))
steps = [chunks[i:i + n] for i in range(0, len(chunks), n)]

for s in tqdm.tqdm(steps):
    with parallel_backend('multiprocessing', n_jobs=max_jobs):
        result = Parallel(n_jobs=-1)(delayed(process_chunk)(BLOCK_PATH, INDEX_PATH, c) for c in s)
    # Write results to database
    for entry in result:
        # Pooling for faster insert
        batch = rocksdb.WriteBatch()
        for e in entry:
            batch.put(e[0].encode(), e[1])
        db.write(batch)

    del result

# Auto-Compaction of database was disabled, so it has to be manually triggered.
db.compact_range()

# Same as above, but the limiting factor is now RAM. Run as many jobs as can fit into memory (15 GB per process),
# but at least one.

n = max(math.floor(mem.available /(15*1024**3)), 1)
chunks = list(range(0, 606590, 1000))
steps = [chunks[i:i + n] for i in range(0, len(chunks), n)]

for s in tqdm.tqdm_notebook(steps):
    with parallel_backend('multiprocessing', n_jobs=n):
        collector = Parallel(n_jobs=-1)(delayed(generate_csv)(BLOCK_PATH, INDEX_PATH, c) for c in s)
    # Extract and flatten data
    collected_addresses = list(map(lambda x: x[0], collector))
    collected_addresses = [item for sublist in collected_addresses for item in sublist]
    collected_blocks = list(map(lambda x: x[1], collector))
    collected_blocks = [item for sublist in collected_blocks for item in sublist]
    collected_transactions = list(map(lambda x: x[2], collector))
    collected_transactions = [item for sublist in collected_transactions for item in sublist]
    collected_before = list(map(lambda x: x[3], collector))
    collected_before = [item for sublist in collected_before for item in sublist]
    collected_belongs = list(map(lambda x: x[4], collector))
    collected_belongs = [item for sublist in collected_belongs for item in sublist]
    collected_receives = list(map(lambda x: x[5], collector))
    collected_receives = [item for sublist in collected_receives for item in sublist]
    collected_sends = list(map(lambda x: x[6], collector))
    collected_sends = [item for sublist in collected_sends for item in sublist]

    # Write CSV files
    address_file_w.writerows(collected_addresses)
    blocks_file_w.writerows(collected_blocks)
    transaction_file_w.writerows(collected_transactions)
    before_file_w.writerows(collected_before)
    belongs_file_w.writerows(collected_belongs)
    receives_file_w.writerows(collected_receives)
    sends_file_w.writerows(collected_sends)

# Close file handles
address_file.close()
blocks_file.close()
transaction_file.close()
before_file.close()
belongs_file.close()
receives_file.close()
sends_file.close()
