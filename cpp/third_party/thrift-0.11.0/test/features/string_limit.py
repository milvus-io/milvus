#!/usr/bin/env python

import argparse
import sys

from util import add_common_args, init_protocol
from local_thrift import thrift  # noqa
from thrift.Thrift import TMessageType, TType


# TODO: generate from ThriftTest.thrift
def test_string(proto, value):
    method_name = 'testString'
    ttype = TType.STRING
    proto.writeMessageBegin(method_name, TMessageType.CALL, 3)
    proto.writeStructBegin(method_name + '_args')
    proto.writeFieldBegin('thing', ttype, 1)
    proto.writeString(value)
    proto.writeFieldEnd()
    proto.writeFieldStop()
    proto.writeStructEnd()
    proto.writeMessageEnd()
    proto.trans.flush()

    _, mtype, _ = proto.readMessageBegin()
    assert mtype == TMessageType.REPLY
    proto.readStructBegin()
    _, ftype, fid = proto.readFieldBegin()
    assert fid == 0
    assert ftype == ttype
    result = proto.readString()
    proto.readFieldEnd()
    _, ftype, _ = proto.readFieldBegin()
    assert ftype == TType.STOP
    proto.readStructEnd()
    proto.readMessageEnd()
    assert value == result


def main(argv):
    p = argparse.ArgumentParser()
    add_common_args(p)
    p.add_argument('--limit', type=int)
    args = p.parse_args()
    proto = init_protocol(args)
    test_string(proto, 'a' * (args.limit - 1))
    test_string(proto, 'a' * (args.limit - 1))
    print('[OK]: limit - 1')
    test_string(proto, 'a' * args.limit)
    test_string(proto, 'a' * args.limit)
    print('[OK]: just limit')
    try:
        test_string(proto, 'a' * (args.limit + 1))
    except Exception:
        print('[OK]: limit + 1')
    else:
        print('[ERROR]: limit + 1')
        assert False


if __name__ == '__main__':
    main(sys.argv[1:])
