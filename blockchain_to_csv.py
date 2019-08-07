import csv
import json
import os
import subprocess
import traceback
from subprocess import Popen

from tqdm import trange


def get_bitcoin_blockcount() -> int:
    """
    Returns the number of blocks currently in the Bitcoin blockchain

    :return:    int, the number of Blocks
    """
    ps = Popen("bitcoin-cli getblockcount", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output: Popen = ps.communicate()[0]
    return int(output.decode("UTF-8"))


def get_blockhash(blocknumber: int) -> str:
    """
    Returns the hash of a Bitcoin-Block, given its blocknumber

    :param blocknumber:     int, the blocknumber
    :return:                str, the Hash
    """
    cmd: str = 'bitcoin-cli getblockhash {0}'.format(str(blocknumber))
    ps = Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output: Popen = ps.communicate()[0]
    return output.decode("UTF-8")


def get_block(blockhash: str) -> dict:
    """
    Returns the transaction data of a bitcoin block in JSON-Format
    :param blockhash:   str, the hash value of the block
    :return:            dict, the contents of the parsed JSON
    """
    cmd: str = 'bitcoin-cli getblock {0}'.format(str(blockhash))
    ps = Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output: Popen = ps.communicate()[0]
    return json.loads(output)


def decode_transaction(raw_transaction: str, block_height: int) -> dict:
    cmd: str = 'bitcoin-cli decoderawtransaction {0}'.format(raw_transaction)
    ps = Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output: Popen = ps.communicate()[0]
    transaction_json: dict = json.loads(output)
    return transaction_json


def get_transaction(transaction_hash: str, block_height: int) -> dict:
    """

    :rtype: object
    """
    cmd: str = 'bitcoin-cli getrawtransaction {0}'.format(transaction_hash)
    ps = Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output: Popen = ps.communicate()[0][:-1]
    output: str = output.decode("UTF-8")
    transaction_details: dict = decode_transaction(output, block_height)
    return transaction_details


start_block = 1
end_block = get_bitcoin_blockcount()

# Initialize CSV files
BASE_PATH: str = '/hdd/bitcoin-csv'

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

address_file_w.writerow(['address'])
transaction_file_w.writerow(['transaction', 'block'])
output_file_w.writerow(['output', 'val'])
output_transaction_file_w.writerow(['output', 'transaction'])
output_address_file_w.writerow(['output', 'address'])

# Debug
# counter = 1
for block_height in trange(start_block, end_block):
    try:
        block_contents = get_block(get_blockhash(block_height))
    except json.decoder.JSONDecodeError as e:
        print('An Error occurred while trying to parse Blockchain \n' + traceback.format_exc())

    for i, tx_hash in enumerate(block_contents['tx']):

        try:
            transaction_data: dict = get_transaction(tx_hash, block_height)
        except json.decoder.JSONDecodeError as e:
            print('An Error occurred while trying to parse Blockchain \n' + traceback.format_exc())

        transaction_file_w.writerow([tx_hash, block_height])
        transaction_output: dict = transaction_data['vout']

        addresses = list(map(lambda x: x['scriptPubKey']['addresses'], transaction_output))
        out_values = list(map(lambda x: [x['n'], x['value']], transaction_output))
        out_transactions = list(map(lambda x: [x['n'], tx_hash], transaction_output))
        out_addresses = list(map(lambda x: [x['n']] + x['scriptPubKey']['addresses'], transaction_output))

        address_file_w.writerows(addresses)
        output_file_w.writerows(out_values)
        output_transaction_file_w.writerows(out_transactions)
        output_address_file_w.writerows(out_addresses)

    # if counter == 10:
    #     break
    # else:
    #     counter += 1

address_file.close()
transaction_file.close()
output_file.close()
output_transaction_file.close()
output_address_file.close()
