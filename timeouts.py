#!/usr/bin/env python3

import json
import os
import queue
import random
import signal
import socket
import struct
import sys
import threading
import time

import redis


_struct_number = struct.Struct("!f")


class Timeout():
    __slots__ = ["deadline", "data"]

    def __init__(self, deadline, data):
        self.deadline = deadline
        self.data = data

    def __lt__(self, other):
        return self.deadline < other.deadline


def _udp_conn(addr):
    so = socket.socket(type=socket.SOCK_DGRAM)
    so.connect(addr)
    return so


class api(object):
    _pack = _struct_number.pack
    _send = _udp_conn(('localhost', 54321)).send
    _wait = redis.StrictRedis(unix_socket_path="run/redis.sock").blpop

    @classmethod
    def schedule(cls, delay, value):
        data = cls._pack(delay) + json.dumps(value).encode()
        try:
            cls._send(data)
        except ConnectionRefusedError:
            pass

    @classmethod
    def ready(cls):
        payload = cls._wait("-timeouts-")
        if payload:
            print(payload)
            return json.loads(payload[1])


def server():
    so = socket.socket(type=socket.SOCK_DGRAM)
    so.bind(('localhost', 54321))

    unpack = _struct_number.unpack
    timeouts = queue.PriorityQueue()

    def consumer():
        sleep_default = 0.02
        rpush = redis.StrictRedis(unix_socket_path="run/redis.sock").rpush
        while True:
            now = time.monotonic()
            wait = sleep_default
            while timeouts.queue:
                deadline = timeouts.queue[0].deadline
                if deadline > now:
                    wait = min(deadline - now, sleep_default)
                    break
                timeout = timeouts.get()
                rpush("-timeouts-", timeout.data)
                #print(data, flush=True)
            #print('sleep', wait, file=sys.stderr)
            time.sleep(wait)
    threading.Thread(target=consumer).start()

    n = _struct_number.size
    while True:
        data = so.recv(1024 * 16)
        delay, = unpack(data[:n])
        deadline = time.monotonic() + delay
        timeouts.put(Timeout(deadline, data[n:]))


def client():
    while True:
        s = input("press any to continue...")
        n = 1000 * 10
        n = 100
        for i in range(n):
            api.schedule(random.random() * 10, i)


def test():
    while True:
        print(api.ready())


if __name__ == "__main__":
    try:
        fn = sys.argv[1]
    except IndexError:
        fn = "?"
    if fn == "s":
        # pypy is better
        server()
    elif fn == "c":
        client()
    elif fn == "t":
        test()
    else:
        pass
