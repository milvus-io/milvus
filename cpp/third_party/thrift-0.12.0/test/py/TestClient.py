#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import os
import sys
import time
import unittest
from optparse import OptionParser

from util import local_libpath

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))


class AbstractTest(unittest.TestCase):
    def setUp(self):
        if options.trans == 'http':
            uri = '{0}://{1}:{2}{3}'.format(('https' if options.ssl else 'http'),
                                            options.host,
                                            options.port,
                                            (options.http_path if options.http_path else '/'))
            if options.ssl:
                __cafile = os.path.join(os.path.dirname(SCRIPT_DIR), "keys", "CA.pem")
                __certfile = os.path.join(os.path.dirname(SCRIPT_DIR), "keys", "client.crt")
                __keyfile = os.path.join(os.path.dirname(SCRIPT_DIR), "keys", "client.key")
                self.transport = THttpClient.THttpClient(uri, cafile=__cafile, cert_file=__certfile, key_file=__keyfile)
            else:
                self.transport = THttpClient.THttpClient(uri)
        else:
            if options.ssl:
                from thrift.transport import TSSLSocket
                socket = TSSLSocket.TSSLSocket(options.host, options.port, validate=False)
            else:
                socket = TSocket.TSocket(options.host, options.port)
            # frame or buffer depending upon args
            self.transport = TTransport.TBufferedTransport(socket)
            if options.trans == 'framed':
                self.transport = TTransport.TFramedTransport(socket)
            elif options.trans == 'buffered':
                self.transport = TTransport.TBufferedTransport(socket)
            elif options.trans == '':
                raise AssertionError('Unknown --transport option: %s' % options.trans)
            if options.zlib:
                self.transport = TZlibTransport.TZlibTransport(self.transport, 9)
        self.transport.open()
        protocol = self.get_protocol(self.transport)
        self.client = ThriftTest.Client(protocol)
        # for multiplexed services:
        protocol2 = self.get_protocol2(self.transport)
        self.client2 = SecondService.Client(protocol2) if protocol2 is not None else None

    def tearDown(self):
        self.transport.close()

    def testVoid(self):
        print('testVoid')
        self.client.testVoid()

    def testString(self):
        print('testString')
        self.assertEqual(self.client.testString('Python' * 20), 'Python' * 20)
        self.assertEqual(self.client.testString(''), '')
        s1 = u'\b\t\n/\\\\\r{}:パイソン"'
        s2 = u"""Afrikaans, Alemannisch, Aragonés, العربية, مصرى,
        Asturianu, Aymar aru, Azərbaycan, Башҡорт, Boarisch, Žemaitėška,
        Беларуская, Беларуская (тарашкевіца), Български, Bamanankan,
        বাংলা, Brezhoneg, Bosanski, Català, Mìng-dĕ̤ng-ngṳ̄, Нохчийн,
        Cebuano, ᏣᎳᎩ, Česky, Словѣ́ньскъ / ⰔⰎⰑⰂⰡⰐⰠⰔⰍⰟ, Чӑвашла, Cymraeg,
        Dansk, Zazaki, ދިވެހިބަސް, Ελληνικά, Emiliàn e rumagnòl, English,
        Esperanto, Español, Eesti, Euskara, فارسی, Suomi, Võro, Føroyskt,
        Français, Arpetan, Furlan, Frysk, Gaeilge, 贛語, Gàidhlig, Galego,
        Avañe'ẽ, ગુજરાતી, Gaelg, עברית, हिन्दी, Fiji Hindi, Hrvatski,
        Kreyòl ayisyen, Magyar, Հայերեն, Interlingua, Bahasa Indonesia,
        Ilokano, Ido, Íslenska, Italiano, 日本語, Lojban, Basa Jawa,
        ქართული, Kongo, Kalaallisut, ಕನ್ನಡ, 한국어, Къарачай-Малкъар,
        Ripoarisch, Kurdî, Коми, Kernewek, Кыргызча, Latina, Ladino,
        Lëtzebuergesch, Limburgs, Lingála, ລາວ, Lietuvių, Latviešu, Basa
        Banyumasan, Malagasy, Македонски, മലയാളം, मराठी, مازِرونی, Bahasa
        Melayu, Nnapulitano, Nedersaksisch, नेपाल भाषा, Nederlands, ‪
        Norsk (nynorsk)‬, ‪Norsk (bokmål)‬, Nouormand, Diné bizaad,
        Occitan, Иронау, Papiamentu, Deitsch, Polski, پنجابی, پښتو,
        Norfuk / Pitkern, Português, Runa Simi, Rumantsch, Romani, Română,
        Русский, Саха тыла, Sardu, Sicilianu, Scots, Sámegiella, Simple
        English, Slovenčina, Slovenščina, Српски / Srpski, Seeltersk,
        Svenska, Kiswahili, தமிழ், తెలుగు, Тоҷикӣ, ไทย, Türkmençe, Tagalog,
        Türkçe, Татарча/Tatarça, Українська, اردو, Tiếng Việt, Volapük,
        Walon, Winaray, 吴语, isiXhosa, ייִדיש, Yorùbá, Zeêuws, 中文,
        Bân-lâm-gú, 粵語"""
        if sys.version_info[0] == 2 and os.environ.get('THRIFT_TEST_PY_NO_UTF8STRINGS'):
            s1 = s1.encode('utf8')
            s2 = s2.encode('utf8')
        self.assertEqual(self.client.testString(s1), s1)
        self.assertEqual(self.client.testString(s2), s2)

    def testMultiplexed(self):
        if self.client2 is not None:
            print('testMultiplexed')
            self.assertEqual(self.client2.secondtestString('foobar'), 'testString("foobar")')

    def testBool(self):
        print('testBool')
        self.assertEqual(self.client.testBool(True), True)
        self.assertEqual(self.client.testBool(False), False)

    def testByte(self):
        print('testByte')
        self.assertEqual(self.client.testByte(63), 63)
        self.assertEqual(self.client.testByte(-127), -127)

    def testI32(self):
        print('testI32')
        self.assertEqual(self.client.testI32(-1), -1)
        self.assertEqual(self.client.testI32(0), 0)

    def testI64(self):
        print('testI64')
        self.assertEqual(self.client.testI64(1), 1)
        self.assertEqual(self.client.testI64(-34359738368), -34359738368)

    def testDouble(self):
        print('testDouble')
        self.assertEqual(self.client.testDouble(-5.235098235), -5.235098235)
        self.assertEqual(self.client.testDouble(0), 0)
        self.assertEqual(self.client.testDouble(-1), -1)
        self.assertEqual(self.client.testDouble(-0.000341012439638598279), -0.000341012439638598279)

    def testBinary(self):
        print('testBinary')
        val = bytearray([i for i in range(0, 256)])
        self.assertEqual(bytearray(self.client.testBinary(bytes(val))), val)

    def testStruct(self):
        print('testStruct')
        x = Xtruct()
        x.string_thing = "Zero"
        x.byte_thing = 1
        x.i32_thing = -3
        x.i64_thing = -5
        y = self.client.testStruct(x)
        self.assertEqual(y, x)

    def testNest(self):
        print('testNest')
        inner = Xtruct(string_thing="Zero", byte_thing=1, i32_thing=-3, i64_thing=-5)
        x = Xtruct2(struct_thing=inner, byte_thing=0, i32_thing=0)
        y = self.client.testNest(x)
        self.assertEqual(y, x)

    def testMap(self):
        print('testMap')
        x = {0: 1, 1: 2, 2: 3, 3: 4, -1: -2}
        y = self.client.testMap(x)
        self.assertEqual(y, x)

    def testSet(self):
        print('testSet')
        x = set([8, 1, 42])
        y = self.client.testSet(x)
        self.assertEqual(y, x)

    def testList(self):
        print('testList')
        x = [1, 4, 9, -42]
        y = self.client.testList(x)
        self.assertEqual(y, x)

    def testEnum(self):
        print('testEnum')
        x = Numberz.FIVE
        y = self.client.testEnum(x)
        self.assertEqual(y, x)

    def testTypedef(self):
        print('testTypedef')
        x = 0xffffffffffffff  # 7 bytes of 0xff
        y = self.client.testTypedef(x)
        self.assertEqual(y, x)

    def testMapMap(self):
        print('testMapMap')
        x = {
            -4: {-4: -4, -3: -3, -2: -2, -1: -1},
            4: {4: 4, 3: 3, 2: 2, 1: 1},
        }
        y = self.client.testMapMap(42)
        self.assertEqual(y, x)

    def testMulti(self):
        print('testMulti')
        xpected = Xtruct(string_thing='Hello2', byte_thing=74, i32_thing=0xff00ff, i64_thing=0xffffffffd0d0)
        y = self.client.testMulti(xpected.byte_thing,
                                  xpected.i32_thing,
                                  xpected.i64_thing,
                                  {0: 'abc'},
                                  Numberz.FIVE,
                                  0xf0f0f0)
        self.assertEqual(y, xpected)

    def testException(self):
        print('testException')
        self.client.testException('Safe')
        try:
            self.client.testException('Xception')
            self.fail("should have gotten exception")
        except Xception as x:
            self.assertEqual(x.errorCode, 1001)
            self.assertEqual(x.message, 'Xception')
            # TODO ensure same behavior for repr within generated python variants
            # ensure exception's repr method works
            # x_repr = repr(x)
            # self.assertEqual(x_repr, 'Xception(errorCode=1001, message=\'Xception\')')

        try:
            self.client.testException('TException')
            self.fail("should have gotten exception")
        except TException as x:
            pass

        # Should not throw
        self.client.testException('success')

    def testMultiException(self):
        print('testMultiException')
        try:
            self.client.testMultiException('Xception', 'ignore')
        except Xception as ex:
            self.assertEqual(ex.errorCode, 1001)
            self.assertEqual(ex.message, 'This is an Xception')

        try:
            self.client.testMultiException('Xception2', 'ignore')
        except Xception2 as ex:
            self.assertEqual(ex.errorCode, 2002)
            self.assertEqual(ex.struct_thing.string_thing, 'This is an Xception2')

        y = self.client.testMultiException('success', 'foobar')
        self.assertEqual(y.string_thing, 'foobar')

    def testOneway(self):
        print('testOneway')
        start = time.time()
        self.client.testOneway(1)  # type is int, not float
        end = time.time()
        self.assertTrue(end - start < 3,
                        "oneway sleep took %f sec" % (end - start))

    def testOnewayThenNormal(self):
        print('testOnewayThenNormal')
        self.client.testOneway(1)  # type is int, not float
        self.assertEqual(self.client.testString('Python'), 'Python')


class MultiplexedOptionalTest(AbstractTest):
    def get_protocol2(self, transport):
        return None


class BinaryTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        return TBinaryProtocol.TBinaryProtocolFactory().getProtocol(transport)


class MultiplexedBinaryTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        wrapped_proto = TBinaryProtocol.TBinaryProtocolFactory().getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "ThriftTest")

    def get_protocol2(self, transport):
        wrapped_proto = TBinaryProtocol.TBinaryProtocolFactory().getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "SecondService")


class AcceleratedBinaryTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        return TBinaryProtocol.TBinaryProtocolAcceleratedFactory(fallback=False).getProtocol(transport)


class MultiplexedAcceleratedBinaryTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        wrapped_proto = TBinaryProtocol.TBinaryProtocolAcceleratedFactory(fallback=False).getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "ThriftTest")

    def get_protocol2(self, transport):
        wrapped_proto = TBinaryProtocol.TBinaryProtocolAcceleratedFactory(fallback=False).getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "SecondService")


class CompactTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        return TCompactProtocol.TCompactProtocolFactory().getProtocol(transport)


class MultiplexedCompactTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        wrapped_proto = TCompactProtocol.TCompactProtocolFactory().getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "ThriftTest")

    def get_protocol2(self, transport):
        wrapped_proto = TCompactProtocol.TCompactProtocolFactory().getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "SecondService")


class AcceleratedCompactTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        return TCompactProtocol.TCompactProtocolAcceleratedFactory(fallback=False).getProtocol(transport)


class MultiplexedAcceleratedCompactTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        wrapped_proto = TCompactProtocol.TCompactProtocolAcceleratedFactory(fallback=False).getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "ThriftTest")

    def get_protocol2(self, transport):
        wrapped_proto = TCompactProtocol.TCompactProtocolAcceleratedFactory(fallback=False).getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "SecondService")


class JSONTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        return TJSONProtocol.TJSONProtocolFactory().getProtocol(transport)


class MultiplexedJSONTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        wrapped_proto = TJSONProtocol.TJSONProtocolFactory().getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "ThriftTest")

    def get_protocol2(self, transport):
        wrapped_proto = TJSONProtocol.TJSONProtocolFactory().getProtocol(transport)
        return TMultiplexedProtocol.TMultiplexedProtocol(wrapped_proto, "SecondService")


class HeaderTest(MultiplexedOptionalTest):
    def get_protocol(self, transport):
        factory = THeaderProtocol.THeaderProtocolFactory()
        return factory.getProtocol(transport)


def suite():
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    if options.proto == 'binary':  # look for --proto on cmdline
        suite.addTest(loader.loadTestsFromTestCase(BinaryTest))
    elif options.proto == 'accel':
        suite.addTest(loader.loadTestsFromTestCase(AcceleratedBinaryTest))
    elif options.proto == 'accelc':
        suite.addTest(loader.loadTestsFromTestCase(AcceleratedCompactTest))
    elif options.proto == 'compact':
        suite.addTest(loader.loadTestsFromTestCase(CompactTest))
    elif options.proto == 'header':
        suite.addTest(loader.loadTestsFromTestCase(HeaderTest))
    elif options.proto == 'json':
        suite.addTest(loader.loadTestsFromTestCase(JSONTest))
    elif options.proto == 'multi':
        suite.addTest(loader.loadTestsFromTestCase(MultiplexedBinaryTest))
    elif options.proto == 'multia':
        suite.addTest(loader.loadTestsFromTestCase(MultiplexedAcceleratedBinaryTest))
    elif options.proto == 'multiac':
        suite.addTest(loader.loadTestsFromTestCase(MultiplexedAcceleratedCompactTest))
    elif options.proto == 'multic':
        suite.addTest(loader.loadTestsFromTestCase(MultiplexedCompactTest))
    elif options.proto == 'multij':
        suite.addTest(loader.loadTestsFromTestCase(MultiplexedJSONTest))
    else:
        raise AssertionError('Unknown protocol given with --protocol: %s' % options.proto)
    return suite


class OwnArgsTestProgram(unittest.TestProgram):
    def parseArgs(self, argv):
        if args:
            self.testNames = args
        else:
            self.testNames = ([self.defaultTest])
        self.createTests()


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('--libpydir', type='string', dest='libpydir',
                      help='include this directory in sys.path for locating library code')
    parser.add_option('--genpydir', type='string', dest='genpydir',
                      help='include this directory in sys.path for locating generated code')
    parser.add_option("--port", type="int", dest="port",
                      help="connect to server at port")
    parser.add_option("--host", type="string", dest="host",
                      help="connect to server")
    parser.add_option("--zlib", action="store_true", dest="zlib",
                      help="use zlib wrapper for compressed transport")
    parser.add_option("--ssl", action="store_true", dest="ssl",
                      help="use SSL for encrypted transport")
    parser.add_option("--http", dest="http_path",
                      help="Use the HTTP transport with the specified path")
    parser.add_option('-v', '--verbose', action="store_const",
                      dest="verbose", const=2,
                      help="verbose output")
    parser.add_option('-q', '--quiet', action="store_const",
                      dest="verbose", const=0,
                      help="minimal output")
    parser.add_option('--protocol', dest="proto", type="string",
                      help="protocol to use, one of: accel, accelc, binary, compact, header, json, multi, multia, multiac, multic, multij")
    parser.add_option('--transport', dest="trans", type="string",
                      help="transport to use, one of: buffered, framed, http")
    parser.set_defaults(framed=False, http_path=None, verbose=1, host='localhost', port=9090, proto='binary')
    options, args = parser.parse_args()

    if options.genpydir:
        sys.path.insert(0, os.path.join(SCRIPT_DIR, options.genpydir))
    sys.path.insert(0, local_libpath())

    if options.http_path:
        options.trans = 'http'

    from ThriftTest import SecondService
    from ThriftTest import ThriftTest
    from ThriftTest.ttypes import Xtruct, Xtruct2, Numberz, Xception, Xception2
    from thrift.Thrift import TException
    from thrift.transport import TTransport
    from thrift.transport import TSocket
    from thrift.transport import THttpClient
    from thrift.transport import TZlibTransport
    from thrift.protocol import TBinaryProtocol
    from thrift.protocol import TCompactProtocol
    from thrift.protocol import THeaderProtocol
    from thrift.protocol import TJSONProtocol
    from thrift.protocol import TMultiplexedProtocol

    OwnArgsTestProgram(defaultTest="suite", testRunner=unittest.TextTestRunner(verbosity=1))
