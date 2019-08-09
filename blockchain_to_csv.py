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
from datetime import datetime

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

# Initialize column names
address_file_w.writerow(['address:ID(Address)'])
address_file_w.writerow(['coinbase'])
blocks_file_w.writerow(['hash:ID(Block)', 'height:int', 'mediantime:timestamp{timezone:UTC}'])
transaction_file_w.writerow(['txid:ID(Transaction)'])
before_file_w.writerow([':START_ID(Block)', ':END_ID(Block)'])
belongs_file_w.writerow([':START_ID(Transaction)', ':END_ID(Block)'])
receives_file_w.writerow([':START_ID(Transaction)', 'value', 'output_nr:int', ':END:ID(Address)'])
sends_file_w.writerow([':START_ID(Address)', 'value', ':END_ID(Transaction)'])


def make_sends_list(transaction: dict) -> list:
    tx_in = transaction['vin']
    txid = transaction['txid']
    inputs = []
    for i, input_tx in enumerate(tx_in):
        if "coinbase" in input_tx:
            input_add_tmp = "coinbase"
            input_value_tmp = transaction['vout'][i]['value']
        else:
            input_raw_tx = con.call('getrawtransaction', input_tx['txid'])
            input_data = con.call('decoderawtransaction', input_raw_tx)
            input_add_tmp = input_data['vout'][input_tx['vout']]['scriptPubKey']['addresses'][0]
            input_value_tmp = input_data['vout'][input_tx['vout']]['value']
        inputs.append([input_add_tmp, input_value_tmp, txid])
    return inputs

for block_height in trange(START_BLOCK, END_BLOCK):
    try:
        # Get Hash of Block at height X of Blockchain
        block_hash = con.call('getblockhash', block_height)
        # Retrieve Information on selected Block
        block_contents = con.call('getblock', block_hash)
        # Extract hash of following block
        next_block_hash = block_contents['nextblockhash']
    except json.decoder.JSONDecodeError as e:
        print('An Error occurred while trying to parse Blockchain \n' + traceback.format_exc())
    median_time = datetime.utcfromtimestamp(block_contents["mediantime"]).strftime('%Y-%m-%dT%H:%M')
    blocks_file_w.writerow([block_hash, block_height, median_time])
    before_file_w.writerow([block_hash, next_block_hash])

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
            receives = list(
                map(lambda x: [tx_hash, x['value'], x['n'], x['scriptPubKey']['addresses'][0]], transaction_output))
            addresses = list(map(lambda x: x['scriptPubKey']['addresses'], transaction_output))
            sends = make_sends_list(transaction_data)
        except KeyError:
            print("Irregular transaction encountered in block {0}".format(str(block_height)))
            continue
        # Write CSV files
        transaction_file_w.writerow([tx_hash])
        belongs_file_w.writerow([tx_hash, block_hash])
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
