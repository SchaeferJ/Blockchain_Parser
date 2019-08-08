"""
A class that provides access to the JSON API of Bitcoin Core.
Based on: https://gist.github.com/Deadlyelder/6baad86e832acf0df23a70914c014d7a#file-bitcoin_rpc_class-py
"""

import json
import time

import requests


class BitcoinConnection:

    def __init__(self, username: str, password: str, max_tries=5):
        self._session = requests.Session()
        self._headers = {'content-type': 'application/json'}
        self._port = '8332'
        self._rpc_user = username
        self._rpc_pw = password
        self._max_tries = max_tries
        self._server_url = 'http://' + self._rpc_user + ':' + self._rpc_pw \
                           + '@localhost:' + self._port

    def expand_request_headers(self, expand_headers: dict):
        """
        Adds additional request headers
        :param expand_headers: A dictionary of the form {'header':'value,...}
        """
        self._headers = {**self._headers, **expand_headers}

    def set_port(self, port: int):
        """
        Allows users to define non-standard ports, e.g. for use with altcoins
        :param port:        int, the port number
        """
        self._port = str(port)
        self._server_url = 'http://' + self._rpc_user + ':' + self._rpc_pw \
                           + '@localhost:' + self._port

    def call(self, method: str, *params):
        """
        Allows the user to send an API Call and returns the response-JSON
        :param method:      str, the method to call
        :param params:      the parameters
        :return:            the API response
        """
        request_payload = json.dumps({'method': method, 'params': list(params), 'jsonrpc': '2.0'})
        encountered_error = False

        for attempts in range(self._max_tries):
            try:
                response = self._session.post(self._server_url, headers=self._headers, data=request_payload)
            except requests.exceptions.ConnectionError:
                if attempts < self._max_tries:
                    print('An Error was encountered while sending the request to the Bitcoin daemon. Will retry...')
                    time.sleep(2)
                else:
                    raise Exception(
                        'Unable to connect to Bitcoin Core. Check if Bitcoin Daemon is running and in server mode')

            if response.status_code != 200:
                raise Exception('Connection Error: Received HTTP Status code {0}'.format(str(response.status_code)))

            response_json = response.json()
            return response_json['result']
