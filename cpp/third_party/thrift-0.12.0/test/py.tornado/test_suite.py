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

import datetime
import glob
import os
import sys
import time
import unittest

basepath = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, basepath + '/gen-py.tornado')
sys.path.insert(0, glob.glob(os.path.join(basepath, '../../lib/py/build/lib*'))[0])

try:
    __import__('tornado')
except ImportError:
    print("module `tornado` not found, skipping test")
    sys.exit(0)

from tornado import gen
from tornado.testing import AsyncTestCase, get_unused_port, gen_test

from thrift import TTornado
from thrift.Thrift import TApplicationException
from thrift.protocol import TBinaryProtocol

from ThriftTest import ThriftTest
from ThriftTest.ttypes import Xception, Xtruct


class TestHandler(object):
    def __init__(self, test_instance):
        self.test_instance = test_instance

    def testVoid(self):
        pass

    def testString(self, s):
        if s == 'unexpected_error':
            raise Exception(s)
        return s

    def testByte(self, b):
        return b

    def testI16(self, i16):
        return i16

    def testI32(self, i32):
        return i32

    def testI64(self, i64):
        return i64

    def testDouble(self, dub):
        return dub

    def testBinary(self, thing):
        return thing

    def testStruct(self, thing):
        return thing

    def testException(self, s):
        if s == 'Xception':
            x = Xception()
            x.errorCode = 1001
            x.message = s
            raise x
        elif s == 'throw_undeclared':
            raise ValueError('testing undeclared exception')

    def testOneway(self, seconds):
        start = time.time()

        def fire_oneway():
            end = time.time()
            self.test_instance.stop((start, end, seconds))

        self.test_instance.io_loop.add_timeout(
            datetime.timedelta(seconds=seconds),
            fire_oneway)
        raise Exception('testing exception in oneway method')

    def testNest(self, thing):
        return thing

    @gen.coroutine
    def testMap(self, thing):
        yield gen.moment
        raise gen.Return(thing)

    def testSet(self, thing):
        return thing

    def testList(self, thing):
        return thing

    def testEnum(self, thing):
        return thing

    def testTypedef(self, thing):
        return thing


class ThriftTestCase(AsyncTestCase):
    def setUp(self):
        super(ThriftTestCase, self).setUp()

        self.port = get_unused_port()

        # server
        self.handler = TestHandler(self)
        self.processor = ThriftTest.Processor(self.handler)
        self.pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        self.server = TTornado.TTornadoServer(self.processor, self.pfactory, io_loop=self.io_loop)
        self.server.bind(self.port)
        self.server.start(1)

        # client
        transport = TTornado.TTornadoStreamTransport('localhost', self.port, io_loop=self.io_loop)
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        self.io_loop.run_sync(transport.open)
        self.client = ThriftTest.Client(transport, pfactory)

    @gen_test
    def test_void(self):
        v = yield self.client.testVoid()
        self.assertEqual(v, None)

    @gen_test
    def test_string(self):
        v = yield self.client.testString('Python')
        self.assertEqual(v, 'Python')

    @gen_test
    def test_byte(self):
        v = yield self.client.testByte(63)
        self.assertEqual(v, 63)

    @gen_test
    def test_i32(self):
        v = yield self.client.testI32(-1)
        self.assertEqual(v, -1)

        v = yield self.client.testI32(0)
        self.assertEqual(v, 0)

    @gen_test
    def test_i64(self):
        v = yield self.client.testI64(-34359738368)
        self.assertEqual(v, -34359738368)

    @gen_test
    def test_double(self):
        v = yield self.client.testDouble(-5.235098235)
        self.assertEqual(v, -5.235098235)

    @gen_test
    def test_struct(self):
        x = Xtruct()
        x.string_thing = "Zero"
        x.byte_thing = 1
        x.i32_thing = -3
        x.i64_thing = -5
        y = yield self.client.testStruct(x)

        self.assertEqual(y.string_thing, "Zero")
        self.assertEqual(y.byte_thing, 1)
        self.assertEqual(y.i32_thing, -3)
        self.assertEqual(y.i64_thing, -5)

    @gen_test
    def test_oneway(self):
        self.client.testOneway(1)
        v = yield self.client.testI32(-1)
        self.assertEqual(v, -1)

    @gen_test
    def test_map(self):
        """
        TestHandler.testMap is a coroutine, this test checks if gen.Return() from a coroutine works.
        """
        expected = {1: 1}
        res = yield self.client.testMap(expected)
        self.assertEqual(res, expected)

    @gen_test
    def test_exception(self):
        try:
            yield self.client.testException('Xception')
        except Xception as ex:
            self.assertEqual(ex.errorCode, 1001)
            self.assertEqual(ex.message, 'Xception')
        else:
            self.fail("should have gotten exception")
        try:
            yield self.client.testException('throw_undeclared')
        except TApplicationException:
            pass
        else:
            self.fail("should have gotten exception")

        yield self.client.testException('Safe')


def suite():
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(ThriftTestCase))
    return suite


if __name__ == '__main__':
    unittest.TestProgram(defaultTest='suite',
                         testRunner=unittest.TextTestRunner(verbosity=1))
