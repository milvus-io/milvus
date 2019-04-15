#!/usr/bin/env python

import argparse
import sys

from util import add_common_args, init_protocol
from local_thrift import thrift  # noqa
from thrift.Thrift import TMessageType, TType


# TODO: generate from ThriftTest.thrift
def test_list(proto, value):
    method_name = 'testList'
    ttype = TType.LIST
    etype = TType.I32
    proto.writeMessageBegin(method_name, TMessageType.CALL, 3)
    proto.writeStructBegin(method_name + '_args')
    proto.writeFieldBegin('thing', ttype, 1)
    proto.writeListBegin(etype, len(value))
    for e in value:
        proto.writeI32(e)
    proto.writeListEnd()
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
    etype2, len2 = proto.readListBegin()
    assert etype == etype2
    assert len2 == len(value)
    for i in range(len2):
        v = proto.readI32()
        assert v == value[i]
    proto.readListEnd()
    proto.readFieldEnd()
    _, ftype, _ = proto.readFieldBegin()
    assert ftype == TType.STOP
    proto.readStructEnd()
    proto.readMessageEnd()


def main(argv):
    p = argparse.ArgumentParser()
    add_common_args(p)
    p.add_argument('--limit', type=int)
    args = p.parse_args()
    proto = init_protocol(args)
    # TODO: test set and map
    test_list(proto, list(range(args.limit - 1)))
    test_list(proto, list(range(args.limit - 1)))
    print('[OK]: limit - 1')
    test_list(proto, list(range(args.limit)))
    test_list(proto, list(range(args.limit)))
    print('[OK]: just limit')
    try:
        test_list(proto, list(range(args.limit + 1)))
    except Exception:
        print('[OK]: limit + 1')
    else:
        print('[ERROR]: limit + 1')
        assert False


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
