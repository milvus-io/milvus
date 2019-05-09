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
from __future__ import division
import logging
import os
import sys
import time
from optparse import OptionParser

from util import local_libpath

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))


class TestHandler(object):
    def testVoid(self):
        if options.verbose > 1:
            logging.info('testVoid()')

    def testString(self, str):
        if options.verbose > 1:
            logging.info('testString(%s)' % str)
        return str

    def testBool(self, boolean):
        if options.verbose > 1:
            logging.info('testBool(%s)' % str(boolean).lower())
        return boolean

    def testByte(self, byte):
        if options.verbose > 1:
            logging.info('testByte(%d)' % byte)
        return byte

    def testI16(self, i16):
        if options.verbose > 1:
            logging.info('testI16(%d)' % i16)
        return i16

    def testI32(self, i32):
        if options.verbose > 1:
            logging.info('testI32(%d)' % i32)
        return i32

    def testI64(self, i64):
        if options.verbose > 1:
            logging.info('testI64(%d)' % i64)
        return i64

    def testDouble(self, dub):
        if options.verbose > 1:
            logging.info('testDouble(%f)' % dub)
        return dub

    def testBinary(self, thing):
        if options.verbose > 1:
            logging.info('testBinary()')  # TODO: hex output
        return thing

    def testStruct(self, thing):
        if options.verbose > 1:
            logging.info('testStruct({%s, %s, %s, %s})' % (thing.string_thing, thing.byte_thing, thing.i32_thing, thing.i64_thing))
        return thing

    def testException(self, arg):
        # if options.verbose > 1:
        logging.info('testException(%s)' % arg)
        if arg == 'Xception':
            raise Xception(errorCode=1001, message=arg)
        elif arg == 'TException':
            raise TException(message='This is a TException')

    def testMultiException(self, arg0, arg1):
        if options.verbose > 1:
            logging.info('testMultiException(%s, %s)' % (arg0, arg1))
        if arg0 == 'Xception':
            raise Xception(errorCode=1001, message='This is an Xception')
        elif arg0 == 'Xception2':
            raise Xception2(
                errorCode=2002,
                struct_thing=Xtruct(string_thing='This is an Xception2'))
        return Xtruct(string_thing=arg1)

    def testOneway(self, seconds):
        if options.verbose > 1:
            logging.info('testOneway(%d) => sleeping...' % seconds)
        time.sleep(seconds / 3)  # be quick
        if options.verbose > 1:
            logging.info('done sleeping')

    def testNest(self, thing):
        if options.verbose > 1:
            logging.info('testNest(%s)' % thing)
        return thing

    def testMap(self, thing):
        if options.verbose > 1:
            logging.info('testMap(%s)' % thing)
        return thing

    def testStringMap(self, thing):
        if options.verbose > 1:
            logging.info('testStringMap(%s)' % thing)
        return thing

    def testSet(self, thing):
        if options.verbose > 1:
            logging.info('testSet(%s)' % thing)
        return thing

    def testList(self, thing):
        if options.verbose > 1:
            logging.info('testList(%s)' % thing)
        return thing

    def testEnum(self, thing):
        if options.verbose > 1:
            logging.info('testEnum(%s)' % thing)
        return thing

    def testTypedef(self, thing):
        if options.verbose > 1:
            logging.info('testTypedef(%s)' % thing)
        return thing

    def testMapMap(self, thing):
        if options.verbose > 1:
            logging.info('testMapMap(%s)' % thing)
        return {
            -4: {
                -4: -4,
                -3: -3,
                -2: -2,
                -1: -1,
            },
            4: {
                4: 4,
                3: 3,
                2: 2,
                1: 1,
            },
        }

    def testInsanity(self, argument):
        if options.verbose > 1:
            logging.info('testInsanity(%s)' % argument)
        return {
            1: {
                2: argument,
                3: argument,
            },
            2: {6: Insanity()},
        }

    def testMulti(self, arg0, arg1, arg2, arg3, arg4, arg5):
        if options.verbose > 1:
            logging.info('testMulti(%s)' % [arg0, arg1, arg2, arg3, arg4, arg5])
        return Xtruct(string_thing='Hello2',
                      byte_thing=arg0, i32_thing=arg1, i64_thing=arg2)


def main(options):
    # set up the protocol factory form the --protocol option
    prot_factories = {
        'binary': TBinaryProtocol.TBinaryProtocolFactory,
        'accel': TBinaryProtocol.TBinaryProtocolAcceleratedFactory,
        'compact': TCompactProtocol.TCompactProtocolFactory,
        'accelc': TCompactProtocol.TCompactProtocolAcceleratedFactory,
        'json': TJSONProtocol.TJSONProtocolFactory,
    }
    pfactory_cls = prot_factories.get(options.proto, None)
    if pfactory_cls is None:
        raise AssertionError('Unknown --protocol option: %s' % options.proto)
    pfactory = pfactory_cls()
    try:
        pfactory.string_length_limit = options.string_limit
        pfactory.container_length_limit = options.container_limit
    except Exception:
        # Ignore errors for those protocols that does not support length limit
        pass

    # get the server type (TSimpleServer, TNonblockingServer, etc...)
    if len(args) > 1:
        raise AssertionError('Only one server type may be specified, not multiple types.')
    server_type = args[0]

    # Set up the handler and processor objects
    handler = TestHandler()
    processor = ThriftTest.Processor(handler)

    # Handle THttpServer as a special case
    if server_type == 'THttpServer':
        server = THttpServer.THttpServer(processor, ('', options.port), pfactory)
        server.serve()
        sys.exit(0)

    # set up server transport and transport factory

    abs_key_path = os.path.join(os.path.dirname(SCRIPT_DIR), 'keys', 'server.pem')

    host = None
    if options.ssl:
        from thrift.transport import TSSLSocket
        transport = TSSLSocket.TSSLServerSocket(host, options.port, certfile=abs_key_path)
    else:
        transport = TSocket.TServerSocket(host, options.port)
    tfactory = TTransport.TBufferedTransportFactory()
    if options.trans == 'buffered':
        tfactory = TTransport.TBufferedTransportFactory()
    elif options.trans == 'framed':
        tfactory = TTransport.TFramedTransportFactory()
    elif options.trans == '':
        raise AssertionError('Unknown --transport option: %s' % options.trans)
    else:
        tfactory = TTransport.TBufferedTransportFactory()
    # if --zlib, then wrap server transport, and use a different transport factory
    if options.zlib:
        transport = TZlibTransport.TZlibTransport(transport)  # wrap  with zlib
        tfactory = TZlibTransport.TZlibTransportFactory()

    # do server-specific setup here:
    if server_type == "TNonblockingServer":
        server = TNonblockingServer.TNonblockingServer(processor, transport, inputProtocolFactory=pfactory)
    elif server_type == "TProcessPoolServer":
        import signal
        from thrift.server import TProcessPoolServer
        server = TProcessPoolServer.TProcessPoolServer(processor, transport, tfactory, pfactory)
        server.setNumWorkers(5)

        def set_alarm():
            def clean_shutdown(signum, frame):
                for worker in server.workers:
                    if options.verbose > 0:
                        logging.info('Terminating worker: %s' % worker)
                    worker.terminate()
                if options.verbose > 0:
                    logging.info('Requesting server to stop()')
                try:
                    server.stop()
                except Exception:
                    pass
            signal.signal(signal.SIGALRM, clean_shutdown)
            signal.alarm(4)
        set_alarm()
    else:
        # look up server class dynamically to instantiate server
        ServerClass = getattr(TServer, server_type)
        server = ServerClass(processor, transport, tfactory, pfactory)
    # enter server main loop
    server.serve()


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--libpydir', type='string', dest='libpydir',
                      help='include this directory to sys.path for locating library code')
    parser.add_option('--genpydir', type='string', dest='genpydir',
                      default='gen-py',
                      help='include this directory to sys.path for locating generated code')
    parser.add_option("--port", type="int", dest="port",
                      help="port number for server to listen on")
    parser.add_option("--zlib", action="store_true", dest="zlib",
                      help="use zlib wrapper for compressed transport")
    parser.add_option("--ssl", action="store_true", dest="ssl",
                      help="use SSL for encrypted transport")
    parser.add_option('-v', '--verbose', action="store_const",
                      dest="verbose", const=2,
                      help="verbose output")
    parser.add_option('-q', '--quiet', action="store_const",
                      dest="verbose", const=0,
                      help="minimal output")
    parser.add_option('--protocol', dest="proto", type="string",
                      help="protocol to use, one of: accel, binary, compact, json")
    parser.add_option('--transport', dest="trans", type="string",
                      help="transport to use, one of: buffered, framed")
    parser.add_option('--container-limit', dest='container_limit', type='int', default=None)
    parser.add_option('--string-limit', dest='string_limit', type='int', default=None)
    parser.set_defaults(port=9090, verbose=1, proto='binary')
    options, args = parser.parse_args()

    # Print TServer log to stdout so that the test-runner can redirect it to log files
    logging.basicConfig(level=options.verbose)

    sys.path.insert(0, os.path.join(SCRIPT_DIR, options.genpydir))
    sys.path.insert(0, local_libpath())

    from ThriftTest import ThriftTest
    from ThriftTest.ttypes import Xtruct, Xception, Xception2, Insanity
    from thrift.Thrift import TException
    from thrift.transport import TTransport
    from thrift.transport import TSocket
    from thrift.transport import TZlibTransport
    from thrift.protocol import TBinaryProtocol
    from thrift.protocol import TCompactProtocol
    from thrift.protocol import TJSONProtocol
    from thrift.server import TServer, TNonblockingServer, THttpServer

    sys.exit(main(options))
