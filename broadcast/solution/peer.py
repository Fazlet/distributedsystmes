#!/usr/bin/env python3

import argparse
import collections
import heapq
import logging

from dslib import Communicator, Message


class Peer:
    def __init__(self, name, addr, peers):
        self._name = name
        self._peers = peers
        self._comm = Communicator(name, addr)
        self._last_received = dict()      # словарь для номеров последнего полученного сообщения от каждого отправителя
        self._seq_no = 0                  # Sequence Number для последнего отправленного сообщения
        self._received = set()            # множество всех полученных сообщений
        self._hold_back_queue = dict()    # очередь сообщений для реализации порядка
    
    def run(self):
        while True:
            msg = self._comm.recv()

            # local user wants to send a message to the chat
            if msg.type == 'SEND' and msg.is_local():
                # basic broadcast
                self._seq_no += 1
                bcast_msg = Message('BCAST', msg.body, {'from': self._name, 'seq_no': self._seq_no,
                                                        'sender': self._name})          # будем добавлять имя процесса,
                                                                                        # который отправил сообщение
                for peer in self._peers:
                    self._comm.send(bcast_msg, peer)

            # received broadcasted message
            elif msg.type == 'BCAST':
                # deliver message to the local user

                # если сообщение уже обработали, то снова обрабатывать его не будем
                if (msg.body not in self._received) and (msg.headers['sender'] != self._name):
                    self._received.add(msg.body)

                    if msg.headers['from'] != self._name:
                        msg.headers['sender'] = self._name
                        for peer in self._peers:              # после получения сообщения отправим его всем другим
                            self._comm.send(msg, peer)

                    # проверка порядка
                    if msg.headers['seq_no'] == (self._last_received.setdefault(msg.headers['from'], 0) + 1):

                        deliver_msg = Message('DELIVER', msg.headers['from'] + ': ' + msg.body)
                        self._comm.send_local(deliver_msg)
                        self._last_received[msg.headers['from']] += 1

                        # чистим очередь
                        while self._hold_back_queue.setdefault(msg.headers['from'], list()):
                            if self._hold_back_queue[msg.headers['from']][0][0] == \
                                    (self._last_received[msg.headers['from']] + 1):

                                # отправляем сообщения из очереди последовательно (по порядку)
                                next_msg = (heapq.heappop(self._hold_back_queue[msg.headers['from']]))[1]
                                deliver_msg = Message('DELIVER', next_msg.headers['from'] +
                                                      ': ' + next_msg.body)
                                self._comm.send_local(deliver_msg)
                                self._last_received[msg.headers['from']] += 1

                            else:             # если в очереди нет следующего сообщения, ничего не делаем
                                break

                    # если N-ое сообщение пришло быстрее, чем предыдущее, то не обрабатываем его, а добавляем в очередь
                    elif msg.headers['seq_no'] > (self._last_received.setdefault(msg.headers['from'], 0) + 1):
                        heapq.heappush(self._hold_back_queue.setdefault(msg.headers['from'], list()), tuple((msg.headers['seq_no'], msg)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='name', 
                        help='peer name (should be unique)', default='peer1')
    parser.add_argument('-l', dest='addr', metavar='host:port', 
                        help='listen on specified address', default='127.0.0.1:9701')
    parser.add_argument('-p', dest='peers', 
                        help='comma separated list of peers', default='127.0.0.1:9701,127.0.0.1:9702')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    peer = Peer(args.name, args.addr, args.peers.split(','))
    peer.run()


if __name__ == "__main__":
    main()
