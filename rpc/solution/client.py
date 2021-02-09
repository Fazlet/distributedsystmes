#!/usr/bin/env python3

import argparse
import json
import logging
import collections

from dslib import Communicator, Message

from common import Store


class StoreProxy(Store):
    """This is client-side proxy for Store service"""

    def __init__(self, server_addr):
        self._client = RpcClient(server_addr)

    def put(self, key, value, overwrite):
        return self._client.call('put', key, value, overwrite)

    def get(self, key):
        return self._client.call('get', key)

    def append(self, key, value):
        return self._client.call('append', key, value)

    def remove(self, key):
        return self._client.call('remove', key)


class RpcClient:
    """This is client-side RPC implementation"""

    def __init__(self, server_addr):
        self._comm = Communicator('client')
        self._server_addr = server_addr

    def call(self, func, *args):
        """Call function on RPC server and return result"""

        packet = list()
        packet.append(func)
        packet.extend(args)                                   # Упаковываем всё в json и помещяем в body сообщения
        msg = Message('REQUEST', body=json.dumps(packet), sender=self._comm._addr)

        if func == 'append':                                  # Для append'а никаких retry'ев, так как неидемпотентен
            self._comm.send(msg, self._server_addr)
            resp = self._comm.recv(1)
            if resp is None:
                raise Exception("Response timeout")
            elif resp.type == 'ERROR':
                raise Exception(resp.body)
            else:
                return resp.body
        else:
            while True:                                        # для идемпотентных отправялем запросы пока не получим ответ
                self._comm.send(msg, self._server_addr)
                resp = self._comm.recv(timeout=1)
                if resp is None:
                    continue
                elif resp.type == 'ERROR':
                    raise Exception(resp.body)
                else:
                    return resp.body


class User:
    """This class mocks a user during tests by invoking proxy functions"""

    def __init__(self, proxy):
        self._proxy = proxy
        # reuse communicator from proxy
        self._comm = proxy._client._comm
    
    def run(self):
        while True:
            msg = self._comm.recv_local()

            # Calls are passed as "CALL func arg1 arg2 ..."

            params = msg.body.split(' ')
            func = params[0]
            args = map(self._parse_arg, params[1:])

            try:
                if func == 'get':
                    result = self._proxy.get(*args)
                elif func == 'put':
                    result = self._proxy.put(*args)
                elif func == 'append':
                    result = self._proxy.append(*args)
                elif func == 'remove':
                    result = self._proxy.remove(*args)
                resp = Message('RESULT', result)
                self._comm.send_local(resp)
            except Exception as err:
                resp = Message('ERROR', str(err))
                self._comm.send_local(resp)

    def _parse_arg(self, arg):
        if arg == 'True':
            return True
        elif arg == 'False':
            return False
        else:
            return arg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='server_addr', metavar='host:port', 
                        help='server address', default='127.0.0.1:9701')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    store = StoreProxy(args.server_addr)
    user = User(store)
    user.run()


if __name__ == "__main__":
    main()
