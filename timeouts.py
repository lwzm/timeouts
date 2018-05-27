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



_sock = socket.socket(type=socket.SOCK_DGRAM)
_sock.connect(('localhost', 54321))

_struct = struct.Struct("!f")
_blpop = redis.StrictRedis(unix_socket_path="run/redis.sock").blpop


class Timeout():
    __slots__ = ["deadline", "data"]

    def __init__(self, deadline, data):
        self.deadline = deadline
        self.data = data

    def __lt__(self, other):
        return self.deadline < other.deadline


def schedule(delay, k, v):
    s = k + "\t" + json.dumps(v)
    data = _struct.pack(delay) + s.encode()
    try:
        _sock.send(data)
    except ConnectionRefusedError:
        pass


def ready(k, timeout=60):
    payload = _blpop(k, timeout)
    if payload:
        return json.loads(payload[1])


def server():
    so = socket.socket(type=socket.SOCK_DGRAM)
    so.bind(('localhost', 54321))

    unpack = struct.Struct("!f").unpack
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
                key, _, data = timeout.data.partition(b'\t')
                rpush(key, data)
                #print(key, data, flush=True)
            #print('sleep', wait, file=sys.stderr)
            time.sleep(wait)
    threading.Thread(target=consumer).start()

    while True:
        data = so.recv(4096)
        delay, = unpack(data[:4])
        deadline = time.monotonic() + delay
        timeouts.put(Timeout(deadline, data[4:]))


def client():
    while True:
        s = input("press any to continue...")
        n = 1000 * 10
        n = 100
        for i in range(n):
            schedule(random.random() * 10, "tt", i)


def test():
    while True:
        print(ready("tt"))


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
