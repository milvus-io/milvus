import argparse
import socket

from local_thrift import thrift  # noqa
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TFramedTransport
from thrift.transport.THttpClient import THttpClient
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TJSONProtocol import TJSONProtocol


def add_common_args(p):
    p.add_argument('--host', default='localhost')
    p.add_argument('--port', type=int, default=9090)
    p.add_argument('--protocol', default='binary')
    p.add_argument('--transport', default='buffered')
    p.add_argument('--ssl', action='store_true')


def parse_common_args(argv):
    p = argparse.ArgumentParser()
    add_common_args(p)
    return p.parse_args(argv)


def init_protocol(args):
    sock = TSocket(args.host, args.port, socket_family=socket.AF_INET)
    sock.setTimeout(500)
    trans = {
        'buffered': TBufferedTransport,
        'framed': TFramedTransport,
        'http': THttpClient,
    }[args.transport](sock)
    trans.open()
    return {
        'binary': TBinaryProtocol,
        'compact': TCompactProtocol,
        'json': TJSONProtocol,
    }[args.protocol](trans)
