#!/usr/bin/env python3

import os
import pickle
import queue
import random
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


def _init_mq(n):
    try:
        from posix_ipc import MessageQueue
        key = f"/.{n}"
    except ImportError:
        from sysv_ipc import MessageQueue
        key = n
    return MessageQueue(key, flags=os.O_CREAT)


class Api(object):
    def __init__(self):
        self._pack = _struct_number.pack
        self._send = _init_mq(54320).send
        self._wait = _init_mq(54321).receive

    def schedule(self, delay, value):
        data = self._pack(delay) + pickle.dumps(value)
        self._send(data, 5)

    def ready(self):
        return self._wait()[0]


api = Api()

def schedule(delay, next, **value):
    value[">"] = next
    api.schedule(delay, value)


def server():
    timeouts = queue.PriorityQueue()

    def consumer():
        sleep_default = 0.02
        send = _init_mq(54321).send
        while True:
            now = time.monotonic()
            secs = sleep_default
            while timeouts.queue:
                deadline = timeouts.queue[0].deadline
                if deadline > now:
                    secs = min(deadline - now, sleep_default)
                    break
                send(timeouts.get().data)
            time.sleep(secs)
    threading.Thread(target=consumer).start()

    n = _struct_number.size
    wait = _init_mq(54320).receive
    unpack = _struct_number.unpack
    while True:
        data = wait()[0]
        delay, = unpack(data[:n])
        deadline = time.monotonic() + delay
        timeouts.put(Timeout(deadline, data[n:]))


def server_():
    import gc
    gc.disable()
    from heapq import heappush, heappop
    from time import monotonic
    from posix_ipc import BusyError
    timeouts = []

    n = _struct_number.size
    unpack = _struct_number.unpack
    wait = _init_mq(54320).receive
    send = _init_mq(54321).send

    while True:
        while timeouts:
            secs = timeouts[0].deadline - monotonic()
            if secs > 0:
                break
            send(heappop(timeouts).data)
        else:
            secs = None
        try:
            data = wait(secs)[0]
        except BusyError:
            continue
        delay, = unpack(data[:n])
        heappush(timeouts, Timeout(monotonic() + delay, data[n:]))


if sys.platform == "linux" and not __debug__:
    server = server_


def client():
    while True:
        s = input("press any to continue...")
        n = 1000 * 10
        n = 100
        for i in range(n):
            api.schedule(random.random() * 10, i)


def test():
    n = 0
    t0 = int(time.monotonic())
    while True:
        print(pickle.loads(api.ready()))
        n += 1
        t1 = int(time.monotonic())
        if t1 != t0:
            print(n, file=sys.stderr)
            t0 = t1
            n = 0


def loop():
    import this
    import traceback
    from pickle import ready

    while True:
        task = pickle.loads(api.ready())
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
