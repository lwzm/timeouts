#!/usr/bin/env python3

import os
import pickle
import queue
import random
import signal
import socket
import struct
import sys
import threading
import time


_struct_number = struct.Struct("!f")


class Timeout():
    __slots__ = ["deadline", "data"]

    def __init__(self, deadline, data):
        self.deadline = deadline
        self.data = data

    def __lt__(self, other):
        return self.deadline < other.deadline


def _init_mq():
    try:
        from posix_ipc import MessageQueue
        key = "/timeouts"
    except ImportError:
        from sysv_ipc import MessageQueue
        key = 54321
    return MessageQueue(key, flags=os.O_CREAT)


class Api(object):
    def __init__(self):
        mq = _init_mq()
        so = socket.socket(type=socket.SOCK_DGRAM)
        so.connect(('localhost', 54321))
        self._pack = _struct_number.pack
        self._send = so.send
        self._wait = mq.receive

    def schedule(self, delay, value):
        data = self._pack(delay) + pickle.dumps(value)
        try:
            self._send(data)
        except ConnectionRefusedError:
            pass

    def ready(self):
        return self._wait()[0]


api = Api()
schedule, ready = api.schedule, api.ready


def server():
    so = socket.socket(type=socket.SOCK_DGRAM)
    so.bind(('localhost', 54321))

    unpack = _struct_number.unpack
    timeouts = queue.PriorityQueue()

    def consumer():
        sleep_default = 0.02
        do = _init_mq().send
        while True:
            now = time.monotonic()
            wait = sleep_default
            while timeouts.queue:
                deadline = timeouts.queue[0].deadline
                if deadline > now:
                    wait = min(deadline - now, sleep_default)
                    break
                timeout = timeouts.get()
                do(timeout.data)
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
            schedule(random.random() * 10, i)


def test():
    while True:
        print(ready())


def loop():
    import this
    import traceback
    while True:
        task = ready()
        try:
            m = task.pop(">")
            m = getattr(this, m)
            m.do(**task)
        except Exception:
            traceback.print_exc()


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
    elif fn == "test":
        test()
    elif fn == "run":
        loop()
    else:
        pass
