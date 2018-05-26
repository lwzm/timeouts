#!/usr/bin/env python3


import json
import random
import socket
import struct

import redis


_struct = struct.Struct("!f")

sock = socket.socket(type=socket.SOCK_DGRAM)
sock.connect(('localhost', 54321))

blpop = redis.StrictRedis(unix_socket_path="run/redis.sock").blpop


def schedule(delay, k, v):
    s = k + "\t" + json.dumps(v)
    data = _struct.pack(delay) + s.encode()
    try:
        sock.send(data)
    except ConnectionRefusedError:
        pass


def ready(k, timeout=60):
    payload = blpop(k, timeout)
    if payload:
        return json.loads(payload[1])


def main():
    #while True:
    #    print(ready("tt"))
    while True:
        s = input("press any to continue...")
        n = 1000 * 10
        n = 100
        for i in range(n):
            schedule(random.random() * 10, "tt", i)


if __name__ == "__main__":
    main()
