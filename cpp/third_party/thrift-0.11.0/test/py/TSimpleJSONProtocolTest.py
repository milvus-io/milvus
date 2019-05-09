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

from ThriftTest.ttypes import Bonk, VersioningTestV1, VersioningTestV2
from thrift.protocol import TJSONProtocol
from thrift.transport import TTransport

import json
import unittest


class SimpleJSONProtocolTest(unittest.TestCase):
    protocol_factory = TJSONProtocol.TSimpleJSONProtocolFactory()

    def _assertDictEqual(self, a, b, msg=None):
        if hasattr(self, 'assertDictEqual'):
            # assertDictEqual only in Python 2.7. Depends on your machine.
            self.assertDictEqual(a, b, msg)
            return

        # Substitute implementation not as good as unittest library's
        self.assertEquals(len(a), len(b), msg)
        for k, v in a.iteritems():
            self.assertTrue(k in b, msg)
            self.assertEquals(b.get(k), v, msg)

    def _serialize(self, obj):
        trans = TTransport.TMemoryBuffer()
        prot = self.protocol_factory.getProtocol(trans)
        obj.write(prot)
        return trans.getvalue()

    def _deserialize(self, objtype, data):
        prot = self.protocol_factory.getProtocol(TTransport.TMemoryBuffer(data))
        ret = objtype()
        ret.read(prot)
        return ret

    def testWriteOnly(self):
        self.assertRaises(NotImplementedError,
                          self._deserialize, VersioningTestV1, b'{}')

    def testSimpleMessage(self):
        v1obj = VersioningTestV1(
            begin_in_both=12345,
            old_string='aaa',
            end_in_both=54321)
        expected = dict(begin_in_both=v1obj.begin_in_both,
                        old_string=v1obj.old_string,
                        end_in_both=v1obj.end_in_both)
        actual = json.loads(self._serialize(v1obj).decode('ascii'))

        self._assertDictEqual(expected, actual)

    def testComplicated(self):
        v2obj = VersioningTestV2(
            begin_in_both=12345,
            newint=1,
            newbyte=2,
            newshort=3,
            newlong=4,
            newdouble=5.0,
            newstruct=Bonk(message="Hello!", type=123),
            newlist=[7, 8, 9],
            newset=set([42, 1, 8]),
            newmap={1: 2, 2: 3},
            newstring="Hola!",
            end_in_both=54321)
        expected = dict(begin_in_both=v2obj.begin_in_both,
                        newint=v2obj.newint,
                        newbyte=v2obj.newbyte,
                        newshort=v2obj.newshort,
                        newlong=v2obj.newlong,
                        newdouble=v2obj.newdouble,
                        newstruct=dict(message=v2obj.newstruct.message,
                                       type=v2obj.newstruct.type),
                        newlist=v2obj.newlist,
                        newset=list(v2obj.newset),
                        newmap=v2obj.newmap,
                        newstring=v2obj.newstring,
                        end_in_both=v2obj.end_in_both)

        # Need to load/dump because map keys get escaped.
        expected = json.loads(json.dumps(expected))
        actual = json.loads(self._serialize(v2obj).decode('ascii'))
        self._assertDictEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
