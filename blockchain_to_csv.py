import json
import sys
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


start_block = 1
end_block = get_bitcoin_blockcount()

for block_height in trange(start_block, end_block):


print(get_bitcoin_blockcount())
print(get_blockhash(1))
print(type(get_block(get_blockhash(1))))
