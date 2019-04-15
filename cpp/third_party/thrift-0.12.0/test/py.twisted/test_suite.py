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

import glob
import os
import sys
import time

basepath = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(basepath, 'gen-py.twisted'))
sys.path.insert(0, glob.glob(os.path.join(basepath, '../../lib/py/build/lib.*'))[0])

from thrift.Thrift import TApplicationException

from ThriftTest import ThriftTest
from ThriftTest.ttypes import Xception, Xtruct
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.internet.protocol import ClientCreator

from zope.interface import implementer


@implementer(ThriftTest.Iface)
class TestHandler:
    def __init__(self):
        self.onewaysQueue = defer.DeferredQueue()

    def testVoid(self):
        pass

    def testString(self, s):
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
        elif s == "throw_undeclared":
            raise ValueError("foo")

    def testOneway(self, seconds):
        def fireOneway(t):
            self.onewaysQueue.put((t, time.time(), seconds))
        reactor.callLater(seconds, fireOneway, time.time())
        raise Exception('')

    def testNest(self, thing):
        return thing

    def testMap(self, thing):
        return thing

    def testSet(self, thing):
        return thing

    def testList(self, thing):
        return thing

    def testEnum(self, thing):
        return thing

    def testTypedef(self, thing):
        return thing


class ThriftTestCase(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.handler = TestHandler()
        self.processor = ThriftTest.Processor(self.handler)
        self.pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        self.server = reactor.listenTCP(
            0, TTwisted.ThriftServerFactory(self.processor, self.pfactory), interface="127.0.0.1")

        self.portNo = self.server.getHost().port

        self.txclient = yield ClientCreator(reactor,
                                            TTwisted.ThriftClientProtocol,
                                            ThriftTest.Client,
                                            self.pfactory).connectTCP("127.0.0.1", self.portNo)
        self.client = self.txclient.client

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.server.stopListening()
        self.txclient.transport.loseConnection()

    @defer.inlineCallbacks
    def testVoid(self):
        self.assertEquals((yield self.client.testVoid()), None)

    @defer.inlineCallbacks
    def testString(self):
        self.assertEquals((yield self.client.testString('Python')), 'Python')

    @defer.inlineCallbacks
    def testByte(self):
        self.assertEquals((yield self.client.testByte(63)), 63)

    @defer.inlineCallbacks
    def testI32(self):
        self.assertEquals((yield self.client.testI32(-1)), -1)
        self.assertEquals((yield self.client.testI32(0)), 0)

    @defer.inlineCallbacks
    def testI64(self):
        self.assertEquals((yield self.client.testI64(-34359738368)), -34359738368)

    @defer.inlineCallbacks
    def testDouble(self):
        self.assertEquals((yield self.client.testDouble(-5.235098235)), -5.235098235)

    # TODO: def testBinary(self) ...

    @defer.inlineCallbacks
    def testStruct(self):
        x = Xtruct()
        x.string_thing = "Zero"
        x.byte_thing = 1
        x.i32_thing = -3
        x.i64_thing = -5
        y = yield self.client.testStruct(x)

        self.assertEquals(y.string_thing, "Zero")
        self.assertEquals(y.byte_thing, 1)
        self.assertEquals(y.i32_thing, -3)
        self.assertEquals(y.i64_thing, -5)

    @defer.inlineCallbacks
    def testException(self):
        try:
            yield self.client.testException('Xception')
            self.fail("should have gotten exception")
        except Xception as x:
            self.assertEquals(x.errorCode, 1001)
            self.assertEquals(x.message, 'Xception')

        try:
            yield self.client.testException("throw_undeclared")
            self.fail("should have gotten exception")
        except TApplicationException:
            pass

        yield self.client.testException('Safe')

    @defer.inlineCallbacks
    def testOneway(self):
        yield self.client.testOneway(1)
        start, end, seconds = yield self.handler.onewaysQueue.get()
        self.assertAlmostEquals(seconds, (end - start), places=1)
        self.assertEquals((yield self.client.testI32(-1)), -1)
