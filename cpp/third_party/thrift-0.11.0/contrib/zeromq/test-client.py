#!/usr/bin/env python
import sys
import time
import zmq
import TZmqClient
import thrift.protocol.TBinaryProtocol
import storage.ttypes
import storage.Storage


def main(args):
    endpoint = "tcp://127.0.0.1:9090"
    socktype = zmq.REQ
    incr = 0
    if len(args) > 1:
        incr = int(args[1])
        if incr:
            socktype = zmq.DOWNSTREAM
            endpoint = "tcp://127.0.0.1:9091"

    ctx = zmq.Context()
    transport = TZmqClient.TZmqClient(ctx, endpoint, socktype)
    protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = storage.Storage.Client(protocol)
    transport.open()

    if incr:
        client.incr(incr)
        time.sleep(0.05)
    else:
        value = client.get()
        print(value)


if __name__ == "__main__":
    main(sys.argv)
