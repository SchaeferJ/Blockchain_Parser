import json
import sys
import traceback
import time
from tqdm import trange
import subprocess
from subprocess import Popen


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

for block_height in trange(start_block, end_block):
    try:
        block_contents = get_block(get_blockhash(block_height))
    except json.decoder.JSONDecodeError as e:
        print('An Error occurred while trying to parse Blockchain \n' + traceback.format_exc())

    for i, tx_hash in enumerate(block_contents['tx']):
        transaction_data: dict = get_transaction(tx_hash,block_height)

print(get_bitcoin_blockcount())
print(get_blockhash(1))
print(type(get_block(get_blockhash(1))))
