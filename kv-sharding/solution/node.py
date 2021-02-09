#!/usr/bin/env python3

import argparse
import hashlib
import logging
import random

from dslib import Message, Process, Runtime


class Node(Process):
    def __init__(self, name):
        super().__init__(name)
        self._group = dict()
        self._alive_list = set()
        self._failed_list = set()
        self._k = 5
        self._checking_node = None
        self._data = dict()

    def target_node(self, key):
        target_node = None
        max_hash = 0
        for node_addr in list(self._alive_list):
            temp_str = key + node_addr
            h = hashlib.md5(temp_str.encode())
            hash_number = (int.from_bytes(h.digest(), byteorder='little'))
            if hash_number > max_hash:
                max_hash = hash_number
                target_node = node_addr
        return target_node

    def receive(self, ctx, msg):

        if msg.is_local():

            # Client commands (API) ***************************************************************

            # Add new node to the system
            # - request body: address of some existing node
            # - response: none
            if msg.type == 'JOIN':
                ctx.set_timer('checkLive', 2)
                ctx.set_timer('checkDead', 10)
                seed = msg.body
                if seed == ctx.addr():
                    # create new empty group and add local node to it
                    self._alive_list.clear()
                    self._alive_list.add(ctx.addr())

                    self._failed_list.clear()

                    self._group.clear()
                    self._group[ctx.addr()] = self.name

                else:
                    # join existing group
                    self._group[ctx.addr()] = self.name
                    self._alive_list.add(ctx.addr())
                    new_msg = Message('JOIN', body=self._group)
                    ctx.send(new_msg, seed)

            # Remove node from the system
            # - request body: none
            # - response: none
            elif msg.type == 'LEAVE':
                self._alive_list.discard(ctx.addr())
                for key in list(self._data.keys()):
                    target = self.target_node(key)
                    data = [key, self._data[key]]
                    new_msg = Message('PUT_IN_YOUR_DATA', body=data)
                    ctx.send(new_msg, target)

                new_msg = Message('LEAVE', body=ctx.addr())
                for member in list(self._alive_list):
                    ctx.send(new_msg, member)
                self._data.clear()
                self._group.clear()
                self._alive_list.clear()
                self._failed_list.clear()

            # Get a list of nodes in the system
            # - request body: none
            # - response: MEMBERS message, body contains the list of all known alive nodes
            elif msg.type == 'GET_MEMBERS':
                ctx.send_local(Message('MEMBERS', list(self._group.values())))

            # Get key value
            # - request body: key
            # - reponse: GET_RESP message, body contains value or empty string if record is not found
            elif msg.type == 'GET':
                key = msg.body
                if key in self._data:
                    new_msg = Message('GET_RESP', body=self._data[key])
                    ctx.send_local(new_msg)
                else:
                    target = self.target_node(key)
                    new_msg = Message('GET', body=key)
                    ctx.send(new_msg, target)

            # Store value for the key
            # - request body: string "key=value"
            # - response: PUT_RESP message, body is empty
            elif msg.type == 'PUT':
                key_and_value = msg.body.split('=')
                key = key_and_value[0]
                value = key_and_value[1]

                target = self.target_node(key)

                if target == ctx.addr():
                    self._data[key] = value
                else:
                    new_msg = Message('PUT_IN_YOUR_DATA', body=[key, value])
                    ctx.send(new_msg, target)

                ctx.set_timer('PUT_RESP', 0.2)

            # Delete value for the key
            # - request body: key
            # - response: DELETE_RESP message, body is empty
            elif msg.type == 'DELETE':
                key = msg.body
                if key in self._data:
                    self._data.pop(key, 0)
                else:
                    target = self.target_node(key)
                    new_msg = Message('DELETE', body=key)
                    ctx.send(new_msg, target)
                ctx.set_timer('DELETE_RESP', 0.2)

            # Get node responsible for the key
            # - request body: key
            # - response: LOOKUP_RESP message, body contains the node name
            elif msg.type == 'LOOKUP':
                key = msg.body
                if key in self._data:
                    new_msg = Message('LOOKUP_RESP', body=self.name)
                    ctx.send_local(new_msg)
                else:
                    target = self.target_node(key)
                    new_msg = Message('LOOKUP_RESP', body=self._group[target])
                    ctx.send_local(new_msg)

            # Get number of records stored on the node
            # - request body: none
            # - response: COUNT_RECRODS_RESP message, body contains the number of stored records
            elif msg.type == 'COUNT_RECORDS':
                new_msg = Message('COUNT_RECORDS_RESP', body=len(list(self._data.keys())))
                ctx.send_local(new_msg)

            # Get keys of records stored on the node
            # - request body: none
            # - response: DUMP_KEYS_RESP message, body contains the list of stored keys
            elif msg.type == 'DUMP_KEYS':
                new_msg = Message('DUMP_KEYS_RESP', body=list(self._data.keys()))
                ctx.send_local(new_msg)

            else:
                err = Message('ERROR', 'unknown command: %s' % msg.type)
                ctx.send_local(err)

        else:

            # Node-to-Node messages ***************************************************************

            # You can introduce any messages for node-to-node communcation

            if msg.type == 'PUT_IN_YOUR_DATA':
                self._data[msg.body[0]] = msg.body[1]

            elif msg.type == 'GET':
                key = msg.body
                if key in self._data:
                    new_msg = Message('GIVE_YOU_DATA', body=self._data[key])
                    ctx.send(new_msg, msg.sender)
                else:
                    new_msg = Message('GIVE_YOU_DATA', body='')
                    ctx.send(new_msg, msg.sender)

            elif msg.type == 'GIVE_YOU_DATA':
                new_msg = Message('GET_RESP', body=msg.body)
                ctx.send_local(new_msg)

            elif msg.type == 'DELETE':
                key = msg.body
                if key in self._data:
                    self._data.pop(key)

            elif msg.type == 'JOIN':
                if self._group != msg.body:
                    self._group.update(msg.body)
                    temp = set(list(msg.body.keys()))
                    self._alive_list.update(temp)

                    for key in list(self._data.keys()):
                        target = self.target_node(key)
                        if target != ctx.addr():
                            new_msg = Message('PUT_IN_YOUR_DATA', body=[key, self._data[key]])
                            ctx.send(new_msg, target)
                            self._data.pop(key)

                    self._failed_list.difference_update(temp)
                    new_msg = Message('JOIN', body=self._group)
                    for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                        ctx.send(new_msg, member)

            elif msg.type == 'LEAVE':
                if msg.body in self._alive_list or msg.body in self._failed_list:
                    self._group.pop(msg.body, 0)
                    self._alive_list.discard(msg.body)
                    self._failed_list.discard(msg.body)
                    for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                        ctx.send(msg, member)

            elif msg.type == 'ARE YOU OKAY?':
                new_msg = Message('I AM OKAY', body=(ctx.addr(), self.name))
                ctx.send(new_msg, msg.body)
                new_msg = Message('JOIN', body=self._group)
                for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                    ctx.send(new_msg, member)

            elif msg.type == 'I AM OKAY':
                if self._checking_node == msg.body[0]:
                    self._checking_node = None
                    ctx.cancel_timer('timeout')
                    ctx.set_timer('checkLive', 2)

            elif msg.type == 'ARE YOU LIVE?':
                new_msg = Message('I LIVE', body=self._group)
                ctx.send(new_msg, msg.body)

            elif msg.type == 'I LIVE':
                self._group.update(msg.body)
                temp = set(list(msg.body.keys()))
                self._alive_list.update(temp)
                self._failed_list.difference_update(temp)

                new_msg = Message('JOIN', body=self._group)
                for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                    ctx.send(new_msg, member)

            elif msg.type == 'HE IS DEAD':
                self._group.pop(msg.body, 0)
                self._alive_list.discard(msg.body)
                self._failed_list.add(msg.body)
                new_msg = Message('KILL HIM', body=list(self._failed_list))
                for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                    ctx.send(new_msg, member)

            elif msg.type == 'KILL HIM':
                temp = set(msg.body)
                if self._failed_list != temp:
                    temp = temp.difference(self._failed_list)
                    if len(temp) > 0:
                        for member in temp:
                            self._group.pop(member, 0)
                        self._alive_list.difference_update(temp)
                        self._failed_list.update(temp)
                    new_msg = Message('KILL HIM', body=list(self._failed_list))
                    for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                        ctx.send(new_msg, member)

            else:
                err = Message('ERROR', 'unknown message: %s' % msg.type)
                ctx.send(err, msg.sender)

    def on_timer(self, ctx, timer):
        if timer == 'checkLive':
            if len(self._alive_list) > 0:
                new_msg = Message('ARE YOU OKAY?', body=ctx.addr())
                self._checking_node = random.choice(list(self._alive_list))
                ctx.send(new_msg, self._checking_node)
                ctx.set_timer('timeout', 2)

        if timer == 'timeout':
            if self._checking_node is not None:
                self._failed_list.add(self._checking_node)
                self._alive_list.discard(self._checking_node)
                self._group.pop(self._checking_node, 0)
                new_msg = Message('HE IS DEAD', body=self._checking_node)
                for member in random.sample(list(self._alive_list), min(self._k, len(list(self._group.keys())))):
                    ctx.send(new_msg, member)
            ctx.set_timer('checkLive', 2)

        if timer == 'checkDead':
            if len(self._failed_list) > 0:
                new_msg = Message('ARE YOU LIVE?', body=ctx.addr())
                seed = random.choice(list(self._failed_list))
                ctx.send(new_msg, seed)
            ctx.set_timer('checkDead', 10)

        if timer == 'PUT_RESP':
            new_msg = Message('PUT_RESP')
            ctx.send_local(new_msg)

        if timer == 'DELETE_RESP':
            new_msg = Message('DELETE_RESP')
            ctx.send_local(new_msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='name', 
                        help='node name (should be unique)', default='1')
    parser.add_argument('-l', dest='addr', metavar='host:port', 
                        help='listen on specified address', default='127.0.0.1:9701')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    node = Node(args.name)
    Runtime(node, args.addr).start()


if __name__ == "__main__":
    main()
