#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

r"""
PYTHONPATH=./gen-py:../../lib/py/build/lib... ./FastbinaryTest.py
"""

# TODO(dreiss): Test error cases.  Check for memory leaks.

from __future__ import print_function

import math
import os
import sys
import timeit

from copy import deepcopy
from pprint import pprint

from thrift.transport import TTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol, TBinaryProtocolAccelerated
from thrift.protocol.TCompactProtocol import TCompactProtocol, TCompactProtocolAccelerated

from DebugProtoTest import Srv
from DebugProtoTest.ttypes import Backwards, Bonk, Empty, HolyMoley, OneOfEach, RandomStuff, Wrapper


class TDevNullTransport(TTransport.TTransportBase):
    def __init__(self):
        pass

    def isOpen(self):
        return True


ooe1 = OneOfEach()
ooe1.im_true = True
ooe1.im_false = False
ooe1.a_bite = 0xd6
ooe1.integer16 = 27000
ooe1.integer32 = 1 << 24
ooe1.integer64 = 6000 * 1000 * 1000
ooe1.double_precision = math.pi
ooe1.some_characters = "Debug THIS!"
ooe1.zomg_unicode = u"\xd7\n\a\t"

ooe2 = OneOfEach()
ooe2.integer16 = 16
ooe2.integer32 = 32
ooe2.integer64 = 64
ooe2.double_precision = (math.sqrt(5) + 1) / 2
ooe2.some_characters = ":R (me going \"rrrr\")"
ooe2.zomg_unicode = u"\xd3\x80\xe2\x85\xae\xce\x9d\x20"\
                    u"\xd0\x9d\xce\xbf\xe2\x85\xbf\xd0\xbe"\
                    u"\xc9\xa1\xd0\xb3\xd0\xb0\xcf\x81\xe2\x84\x8e"\
                    u"\x20\xce\x91\x74\x74\xce\xb1\xe2\x85\xbd\xce\xba"\
                    u"\xc7\x83\xe2\x80\xbc"

if sys.version_info[0] == 2 and os.environ.get('THRIFT_TEST_PY_NO_UTF8STRINGS'):
    ooe1.zomg_unicode = ooe1.zomg_unicode.encode('utf8')
    ooe2.zomg_unicode = ooe2.zomg_unicode.encode('utf8')

hm = HolyMoley(**{"big": [], "contain": set(), "bonks": {}})
hm.big.append(ooe1)
hm.big.append(ooe2)
hm.big[0].a_bite = 0x22
hm.big[1].a_bite = 0x22

hm.contain.add(("and a one", "and a two"))
hm.contain.add(("then a one, two", "three!", "FOUR!"))
if sys.version_info[0] == 2 and os.environ.get('THRIFT_TEST_PY_NO_UTF8STRINGS'):
    hm.contain.add((u"\xd7\n\a\t".encode('utf8'),))
else:
    hm.contain.add((u"\xd7\n\a\t",))
hm.contain.add(())

hm.bonks["nothing"] = []
hm.bonks["something"] = [
    Bonk(**{"type": 1, "message": "Wait."}),
    Bonk(**{"type": 2, "message": "What?"}),
]
hm.bonks["poe"] = [
    Bonk(**{"type": 3, "message": "quoth"}),
    Bonk(**{"type": 4, "message": "the raven"}),
    Bonk(**{"type": 5, "message": "nevermore"}),
]

rs = RandomStuff()
rs.a = 1
rs.b = 2
rs.c = 3
rs.myintlist = list(range(20))
rs.maps = {1: Wrapper(**{"foo": Empty()}), 2: Wrapper(**{"foo": Empty()})}
rs.bigint = 124523452435
rs.triple = 3.14

# make sure this splits two buffers in a buffered protocol
rshuge = RandomStuff()
rshuge.myintlist = list(range(10000))

my_zero = Srv.Janky_result(**{"success": 5})


class Test(object):
    def __init__(self, fast, slow):
        self._fast = fast
        self._slow = slow

    def _check_write(self, o):
        trans_fast = TTransport.TMemoryBuffer()
        trans_slow = TTransport.TMemoryBuffer()
        prot_fast = self._fast(trans_fast, fallback=False)
        prot_slow = self._slow(trans_slow)

        o.write(prot_fast)
        o.write(prot_slow)
        ORIG = trans_slow.getvalue()
        MINE = trans_fast.getvalue()
        if ORIG != MINE:
            print("actual  : %s\nexpected: %s" % (repr(MINE), repr(ORIG)))
            raise Exception('write value mismatch')

    def _check_read(self, o):
        prot = self._slow(TTransport.TMemoryBuffer())
        o.write(prot)

        slow_version_binary = prot.trans.getvalue()

        prot = self._fast(
            TTransport.TMemoryBuffer(slow_version_binary), fallback=False)
        c = o.__class__()
        c.read(prot)
        if c != o:
            print("actual  : ")
            pprint(repr(c))
            print("expected: ")
            pprint(repr(o))
            raise Exception('read value mismatch')

        prot = self._fast(
            TTransport.TBufferedTransport(
                TTransport.TMemoryBuffer(slow_version_binary)), fallback=False)
        c = o.__class__()
        c.read(prot)
        if c != o:
            print("actual  : ")
            pprint(repr(c))
            print("expected: ")
            pprint(repr(o))
            raise Exception('read value mismatch')

    def do_test(self):
        self._check_write(HolyMoley())
        self._check_read(HolyMoley())

        self._check_write(hm)
        no_set = deepcopy(hm)
        no_set.contain = set()
        self._check_read(no_set)
        self._check_read(hm)

        self._check_write(rs)
        self._check_read(rs)

        self._check_write(rshuge)
        self._check_read(rshuge)

        self._check_write(my_zero)
        self._check_read(my_zero)

        self._check_read(Backwards(**{"first_tag2": 4, "second_tag1": 2}))

        # One case where the serialized form changes, but only superficially.
        o = Backwards(**{"first_tag2": 4, "second_tag1": 2})
        trans_fast = TTransport.TMemoryBuffer()
        trans_slow = TTransport.TMemoryBuffer()
        prot_fast = self._fast(trans_fast, fallback=False)
        prot_slow = self._slow(trans_slow)

        o.write(prot_fast)
        o.write(prot_slow)
        ORIG = trans_slow.getvalue()
        MINE = trans_fast.getvalue()
        assert id(ORIG) != id(MINE)

        prot = self._fast(TTransport.TMemoryBuffer(), fallback=False)
        o.write(prot)
        prot = self._slow(
            TTransport.TMemoryBuffer(prot.trans.getvalue()))
        c = o.__class__()
        c.read(prot)
        if c != o:
            print("copy: ")
            pprint(repr(c))
            print("orig: ")
            pprint(repr(o))


def do_test(fast, slow):
    Test(fast, slow).do_test()


def do_benchmark(protocol, iters=5000, skip_slow=False):
    setup = """
from __main__ import hm, rs, TDevNullTransport
from thrift.protocol.{0} import {0}{1}
trans = TDevNullTransport()
prot = {0}{1}(trans{2})
"""

    setup_fast = setup.format(protocol, 'Accelerated', ', fallback=False')
    if not skip_slow:
        setup_slow = setup.format(protocol, '', '')

    print("Starting Benchmarks")

    if not skip_slow:
        print("HolyMoley Standard = %f" %
              timeit.Timer('hm.write(prot)', setup_slow).timeit(number=iters))

    print("HolyMoley Acceler. = %f" %
          timeit.Timer('hm.write(prot)', setup_fast).timeit(number=iters))

    if not skip_slow:
        print("FastStruct Standard = %f" %
              timeit.Timer('rs.write(prot)', setup_slow).timeit(number=iters))

    print("FastStruct Acceler. = %f" %
          timeit.Timer('rs.write(prot)', setup_fast).timeit(number=iters))


if __name__ == '__main__':
    print('Testing TBinaryAccelerated')
    do_test(TBinaryProtocolAccelerated, TBinaryProtocol)
    do_benchmark('TBinaryProtocol')
    print('Testing TCompactAccelerated')
    do_test(TCompactProtocolAccelerated, TCompactProtocol)
    do_benchmark('TCompactProtocol')
