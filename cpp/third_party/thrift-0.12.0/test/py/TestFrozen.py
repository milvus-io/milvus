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

from DebugProtoTest.ttypes import CompactProtoTestStruct, Empty, Wrapper
from thrift.Thrift import TFrozenDict
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol
import collections
import unittest


class TestFrozenBase(unittest.TestCase):
    def _roundtrip(self, src, dst):
        otrans = TTransport.TMemoryBuffer()
        optoro = self.protocol(otrans)
        src.write(optoro)
        itrans = TTransport.TMemoryBuffer(otrans.getvalue())
        iproto = self.protocol(itrans)
        return dst.read(iproto) or dst

    def test_dict_is_hashable_only_after_frozen(self):
        d0 = {}
        self.assertFalse(isinstance(d0, collections.Hashable))
        d1 = TFrozenDict(d0)
        self.assertTrue(isinstance(d1, collections.Hashable))

    def test_struct_with_collection_fields(self):
        pass

    def test_set(self):
        """Test that annotated set field can be serialized and deserialized"""
        x = CompactProtoTestStruct(set_byte_map={
            frozenset([42, 100, -100]): 99,
            frozenset([0]): 100,
            frozenset([]): 0,
        })
        x2 = self._roundtrip(x, CompactProtoTestStruct())
        self.assertEqual(x2.set_byte_map[frozenset([42, 100, -100])], 99)
        self.assertEqual(x2.set_byte_map[frozenset([0])], 100)
        self.assertEqual(x2.set_byte_map[frozenset([])], 0)

    def test_map(self):
        """Test that annotated map field can be serialized and deserialized"""
        x = CompactProtoTestStruct(map_byte_map={
            TFrozenDict({42: 42, 100: -100}): 99,
            TFrozenDict({0: 0}): 100,
            TFrozenDict({}): 0,
        })
        x2 = self._roundtrip(x, CompactProtoTestStruct())
        self.assertEqual(x2.map_byte_map[TFrozenDict({42: 42, 100: -100})], 99)
        self.assertEqual(x2.map_byte_map[TFrozenDict({0: 0})], 100)
        self.assertEqual(x2.map_byte_map[TFrozenDict({})], 0)

    def test_list(self):
        """Test that annotated list field can be serialized and deserialized"""
        x = CompactProtoTestStruct(list_byte_map={
            (42, 100, -100): 99,
            (0,): 100,
            (): 0,
        })
        x2 = self._roundtrip(x, CompactProtoTestStruct())
        self.assertEqual(x2.list_byte_map[(42, 100, -100)], 99)
        self.assertEqual(x2.list_byte_map[(0,)], 100)
        self.assertEqual(x2.list_byte_map[()], 0)

    def test_empty_struct(self):
        """Test that annotated empty struct can be serialized and deserialized"""
        x = CompactProtoTestStruct(empty_struct_field=Empty())
        x2 = self._roundtrip(x, CompactProtoTestStruct())
        self.assertEqual(x2.empty_struct_field, Empty())

    def test_struct(self):
        """Test that annotated struct can be serialized and deserialized"""
        x = Wrapper(foo=Empty())
        self.assertEqual(x.foo, Empty())
        x2 = self._roundtrip(x, Wrapper)
        self.assertEqual(x2.foo, Empty())


class TestFrozen(TestFrozenBase):
    def protocol(self, trans):
        return TBinaryProtocol.TBinaryProtocolFactory().getProtocol(trans)


class TestFrozenAcceleratedBinary(TestFrozenBase):
    def protocol(self, trans):
        return TBinaryProtocol.TBinaryProtocolAcceleratedFactory(fallback=False).getProtocol(trans)


class TestFrozenAcceleratedCompact(TestFrozenBase):
    def protocol(self, trans):
        return TCompactProtocol.TCompactProtocolAcceleratedFactory(fallback=False).getProtocol(trans)


def suite():
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestFrozen))
    suite.addTest(loader.loadTestsFromTestCase(TestFrozenAcceleratedBinary))
    suite.addTest(loader.loadTestsFromTestCase(TestFrozenAcceleratedCompact))
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest="suite", testRunner=unittest.TextTestRunner(verbosity=2))
