#!/usr/bin/env python3

"""
test asyncio and uvloop
"""

import os
import random
import struct
import sys
import time

import asyncio
import uvloop
#asyncio.set_event_loop(uvloop.new_event_loop())

_struct_number = struct.Struct("!f")


def _init_mq(n, **kw):
    try:
        from posix_ipc import MessageQueue
        key = f"/.{n}"
    except ImportError:
        from sysv_ipc import MessageQueue
        key = n
    return MessageQueue(key, flags=os.O_CREAT, **kw)


def server():
    loop = asyncio.get_event_loop()
    _0 = _init_mq(54320, write=False)
    _1 = _init_mq(54321, read=False)

    n = _struct_number.size
    unpack = _struct_number.unpack

    def send(data):
        _1.send(data)

    def receive():
        data = _0.receive()[0]
        delay, = unpack(data[:n])
        loop.call_later(delay, send, data[n:])

    loop.add_reader(_0.mqd, receive)
    loop.run_forever()


if __name__ == "__main__":
    server()
