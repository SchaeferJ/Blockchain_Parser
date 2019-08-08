# Bitcoin to Neo4j

The code in this repository is an end-to-end solution for loading all bitcoin transactions in the  blockchain into a 
Neo4j graph database. 

## Considerations
When running this script, you will essentially turn your computer into a _full node_ of the bitcoin network. This means
that you will have to **download the ENTIRE (!) blockchain** (> 230 GB) on your PC. Make sure that you have sufficient
space on your hard drive and that your internet connection is not metered. 

**Depending on your bandwidth and your system configuration, this script might take between 2-7 days to complete.**

## Instructions

**1. Install Neo4j**
  * On Debian/Ubuntu:  `$ sudo apt-get install neo4j`
  * All others click [here](https://neo4j.com/docs/operations-manual/current/installation/) for Instructions

**2. Install Bitcoin Core**
  * [Linux](https://bitcoin.org/en/full-node#linux-instructions) (Install Daemon only)
  * [MacOS](https://bitcoin.org/en/full-node#mac-os-x-instructions)
  * [Windows](https://bitcoin.org/en/full-node#windows-instructions)

**3. Configure the bitcoin client**

Prior to running your bitcoin node for the first time, you should adjust the bitcoin.conf file as follows:

```
txindex=1
server=1
rpcuser=<YOUR_USERNAME>
rpcpassword=<YOUR_PASSWORD>
```
Typical locations for the bitcoin.conf file are:
```
Windows:        C:\Users\<username>\AppData\Roaming\Bitcoin                                             
Linux:          /home/<username>/.bitcoin/bitcoin.conf                                                           
MacOS:          /Users/<username>/Library/Application Support/Bitcoin/bitcoin.conf
```
**4. Run the Bitcoin client**
  * Linux: `$ bitcoind --daemon`
  * MacOS: `bitcoind -daemon`
  * Windows: Open command prompt and tpye `C:\Program Files\Bitcoin\daemon\bitcoind`
 
**5. Install this Repo**

Download or clone this repository, save it in the place of your choice and install the dependencies by running

`pip install -r requirements.txt`

