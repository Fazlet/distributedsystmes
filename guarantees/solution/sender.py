#!/usr/bin/env python

import argparse
import logging
import collections

from dslib import Communicator, Message


class Sender:
    def __init__(self, name, recv_addr):
        self._comm = Communicator(name)
        self._recv_addr = recv_addr
        self._local_messages = collections.deque()                  # очередь локальных сообщений

    def run(self):
        while True:
            try:                                                    # берём очередное локальное сообщение
                msg = self._local_messages.popleft()
            except IndexError:
                msg = self._comm.recv_local()

            # deliver INFO-1 message to receiver user
            # underlying transport: unreliable with possible repetitions
            # goal: receiver knows all that were recieved but at most once
            if msg.type == 'INFO-1':
                self._comm.send(msg, self._recv_addr)                       # просто отправляем

            # deliver INFO-2 message to receiver user
            # underlying transport: unreliable with possible repetitions
            # goal: receiver knows all at least once
            elif msg.type == 'INFO-2':
                while True:                                                 # отправляем и ждём ответ;
                    self._comm.send(msg, self._recv_addr)                   # если ответ - сообщение с локального сервера,
                    resp = self._comm.recv(timeout=0.5)                     # то добавляем его в очередь и дальше ждём ответ
                    if resp is None:                                        # от receiver'а (с повторной отправкой)
                        continue
                    elif resp.is_local():
                        self._local_messages.append(resp)
                    else:
                        resp._sender = msg._sender
                        if resp == msg:
                            break

            # deliver INFO-3 message to receiver user
            # underlying transport: unreliable with possible repetitions
            # goal: receiver knows all exactly once
            elif msg.type == 'INFO-3':
                while True:                                                 # для INFO-3 логика такая же, как для INFO-2,
                    self._comm.send(msg, self._recv_addr)                   # отличия есть только у receiver'a
                    resp = self._comm.recv(timeout=0.5)
                    if resp is None:
                        continue
                    elif resp.is_local():
                        self._local_messages.append(resp)
                    else:
                        resp._sender = msg._sender
                        if resp == msg:
                            break

            # deliver INFO-4 message to receiver user
            # underlying transport: unreliable with possible repetitions
            # goal: receiver knows all exactly once in the order
            elif msg.type == 'INFO-4':
                while True:                                                   # для INFO-4 логика такая же, как для INFO-2,
                    self._comm.send(msg, self._recv_addr)                     # отличия есть только у receiver'a
                    resp = self._comm.recv(timeout=0.5)
                    if resp is None:
                        continue
                    elif resp.is_local():                                     # порядок сохраняется потому, что новые
                        self._local_messages.append(resp)                     # локальные сообщения мы не отправляем до тех пор
                    else:                                                     # пока не получим ответ от receiver'а
                        resp._sender = msg._sender
                        if resp == msg:
                            break

            else:
                err = Message('ERROR', 'unknown command: %s' % msg.type)
                self._comm.send_local(err)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='recv_addr', metavar='host:port',
                        help='receiver address', default='127.0.0.1:9701')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    sender = Sender('sender', args.recv_addr)
    sender.run()


if __name__ == "__main__":
    main()
