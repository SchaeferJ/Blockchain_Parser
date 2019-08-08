"""
Blockchain-Parser: Iterates over the Bitcoin blockchain using Bitcoin Core's RPC API, extracts relevant transaction
data and stores it in multiple CSV-Files.

Requires an installation of Bitcoin Core running a full node with a **fully** indexed blockchain.
"""

import argparse
import csv
import json
import os
import traceback

from tqdm import trange

import bitcoin_core_api as btc

# Parse command-line arguments
ap = argparse.ArgumentParser()
ap.add_argument("--startblock", help="Block to start with, defaults to 1", type=int, default=1)
ap.add_argument("--endblock", help="Block to stop at, defaults to full length of blockchain", type=int,
                default=-1)
ap.add_argument("--dir", help="Directory to store the CSVs in. Defaults to current working directory",
                type=str, default="")
ap.add_argument("--port", help="Port of RPC Server. Defaults to 8332",
                type=int, default=8332)
ap.add_argument("--uname", help="RPC username. Defaults to alice",
                type=str, default="alice")
ap.add_argument("--pw", help="RPC password. Defaults to wonderland",
                type=str, default="wonderland")
args = vars(ap.parse_args())

# Initialize Connection to Bitcoin Core
con = btc.BitcoinConnection(username=args['uname'], password=args['pw'])

if args['port'] != 8332:
    con.set_port(args['port'])

# Initialize global constants from CLI arguments
START_BLOCK: int = args['startblock']

if args['endblock'] == -1:
    END_BLOCK: int = con.call("getblockcount")
else:
    END_BLOCK: int = args['endblock']

if args['dir'] == "":
    BASE_PATH: str = os.getcwd()
else:
    BASE_PATH: str = args['dir']

# Create output files
address_file = open(os.path.join(BASE_PATH, 'addresses.csv'), 'w')
address_file_w = csv.writer(address_file)
transaction_file = open(os.path.join(BASE_PATH, 'transactions.csv'), 'w')
transaction_file_w = csv.writer(transaction_file)
output_file = open(os.path.join(BASE_PATH, 'outputs.csv'), 'w')
output_file_w = csv.writer(output_file)
output_transaction_file = open(os.path.join(BASE_PATH, 'outputs-transactions.csv'), 'w')
output_transaction_file_w = csv.writer(output_transaction_file)
output_address_file = open(os.path.join(BASE_PATH, 'outputs-addresses.csv'), 'w')
output_address_file_w = csv.writer(output_address_file)

# Initialize column names
address_file_w.writerow(['address'])
transaction_file_w.writerow(['transaction', 'block'])
output_file_w.writerow(['output', 'val'])
output_transaction_file_w.writerow(['output', 'transaction'])
output_address_file_w.writerow(['output', 'address'])

for block_height in trange(START_BLOCK, END_BLOCK):
    try:
        # Get Hash of Block at height X of Blockchain
        block_hash = con.call('getblockhash', block_height)
        # Retrieve Information on selected Block
        block_contents = con.call('getblock', block_hash)
    except json.decoder.JSONDecodeError as e:
        print('An Error occurred while trying to parse Blockchain \n' + traceback.format_exc())

    # Iterates over all Transactions stored in Block
    for i, tx_hash in enumerate(block_contents['tx']):
        try:
            # Get raw transaction data by hash (serialized, hex encoded)
            raw_tx: str = con.call('getrawtransaction', tx_hash)
            # Decode hex data to JSON
            transaction_data = con.call('decoderawtransaction', raw_tx)
        except json.decoder.JSONDecodeError as e:
            print('An Error occurred while trying to parse Blockchain \n' + traceback.format_exc())

        # Create data structures for the CSV files
        transaction_output: dict = transaction_data['vout']

        # Skip weird transcations that are probably either bugs or attacks on the bitcoin network
        # E.g. Transaction 2a0597e665ac3d1cabeede95cedf907934db7f639e477b3c77b242140d8cf728 in Block #71036
        try:
            addresses = list(map(lambda x: x['scriptPubKey']['addresses'], transaction_output))
            out_values = list(map(lambda x: [x['n'], x['value']], transaction_output))
            out_transactions = list(map(lambda x: [x['n'], tx_hash], transaction_output))
            out_addresses = list(map(lambda x: [x['n']] + x['scriptPubKey']['addresses'], transaction_output))
        except KeyError:
            print("Irregular transaction encountered in block {0}".format(str(block_height)))
            continue
        # Write CSV files
        transaction_file_w.writerow([tx_hash, block_height])
        address_file_w.writerows(addresses)
        output_file_w.writerows(out_values)
        output_transaction_file_w.writerows(out_transactions)
        output_address_file_w.writerows(out_addresses)

# Finalize
address_file.close()
transaction_file.close()
output_file.close()
output_transaction_file.close()
output_address_file.close()
