#!/usr/bin/env python
import zmq
import TZmqServer
import storage.ttypes
import storage.Storage


class StorageHandler(storage.Storage.Iface):
    def __init__(self):
        self.value = 0

    def incr(self, amount):
        self.value += amount

    def get(self):
        return self.value


def main():
    handler = StorageHandler()
    processor = storage.Storage.Processor(handler)

    ctx = zmq.Context()
    reqrep_server = TZmqServer.TZmqServer(processor, ctx, "tcp://0.0.0.0:9090", zmq.REP)
    oneway_server = TZmqServer.TZmqServer(processor, ctx, "tcp://0.0.0.0:9091", zmq.UPSTREAM)
    multiserver = TZmqServer.TZmqMultiServer()
    multiserver.servers.append(reqrep_server)
    multiserver.servers.append(oneway_server)
    multiserver.serveForever()


if __name__ == "__main__":
    main()
