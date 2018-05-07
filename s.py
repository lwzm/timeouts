#!/usr/bin/env python3
# pypy is better


import os
import queue
import socket
import struct
import sys
import threading
import time

import redis


class Timeout():
    __slots__ = ["deadline", "data"]

    def __init__(self, deadline, data):
        self.deadline = deadline
        self.data = data

    def __lt__(self, other):
        return self.deadline < other.deadline


def main():
    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.bind(('localhost', 1111))

    n_procs = int(os.getenv("N", 1))
    l_procs = []
    for i in range(n_procs):
        pid = os.fork()
        if not pid:
            break
        l_procs.append(pid)
    else:
        try:
            while True:
                input()
        except (Exception, KeyboardInterrupt) as e:
            import traceback
            traceback.print_exc()
        for pid in l_procs:
            os.kill(pid, 15)
            os.wait()
        sys.exit()

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
        data = sock.recv(4096)
        delay, = unpack(data[:4])
        deadline = time.monotonic() + delay
        timeouts.put(Timeout(deadline, data[4:]))


if __name__ == "__main__":
    main()
