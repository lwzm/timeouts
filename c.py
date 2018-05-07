#!/usr/bin/env python3



import os
import time
import socket
import struct
import random


_struct = struct.Struct("!f")


def main():
    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.connect(('localhost', 1111))

    while True:
        s = input()
        for i in range(1000*10):
            sock.send(_struct.pack(random.random() * 10) + f"t\t{i}".encode())


if __name__ == "__main__":
    main()
