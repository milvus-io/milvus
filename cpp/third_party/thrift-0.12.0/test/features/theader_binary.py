#!/usr/bin/env python

import argparse
import socket
import sys

from util import add_common_args
from local_thrift import thrift  # noqa
from thrift.Thrift import TMessageType, TType
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TFramedTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol


def test_void(proto):
    proto.writeMessageBegin('testVoid', TMessageType.CALL, 3)
    proto.writeStructBegin('testVoid_args')
    proto.writeFieldStop()
    proto.writeStructEnd()
    proto.writeMessageEnd()
    proto.trans.flush()

    _, mtype, _ = proto.readMessageBegin()
    assert mtype == TMessageType.REPLY
    proto.readStructBegin()
    _, ftype, _ = proto.readFieldBegin()
    assert ftype == TType.STOP
    proto.readStructEnd()
    proto.readMessageEnd()


# THeader stack should accept binary protocol with optionally framed transport
def main(argv):
    p = argparse.ArgumentParser()
    add_common_args(p)
    # Since THeaderTransport acts as framed transport when detected frame, we
    # cannot use --transport=framed as it would result in 2 layered frames.
    p.add_argument('--override-transport')
    p.add_argument('--override-protocol')
    args = p.parse_args()
    assert args.protocol == 'header'
    assert args.transport == 'buffered'
    assert not args.ssl

    sock = TSocket(args.host, args.port, socket_family=socket.AF_INET)
    if not args.override_transport or args.override_transport == 'buffered':
        trans = TBufferedTransport(sock)
    elif args.override_transport == 'framed':
        print('TFRAMED')
        trans = TFramedTransport(sock)
    else:
        raise ValueError('invalid transport')
    trans.open()

    if not args.override_protocol or args.override_protocol == 'binary':
        proto = TBinaryProtocol(trans)
    elif args.override_protocol == 'compact':
        proto = TCompactProtocol(trans)
    else:
        raise ValueError('invalid transport')

    test_void(proto)
    test_void(proto)

    trans.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
