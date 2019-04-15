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
import logging
import sys

sys.path.append('gen-py.tornado')
sys.path.insert(0, glob.glob('../../lib/py/build/lib*')[0])

from tutorial import Calculator
from tutorial.ttypes import Operation, Work, InvalidOperation

from thrift import TTornado
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from tornado import gen
from tornado import ioloop


@gen.coroutine
def communicate():
    # create client
    transport = TTornado.TTornadoStreamTransport('localhost', 9090)
    # open the transport, bail on error
    try:
        yield transport.open()
        print('Transport is opened')
    except TTransport.TTransportException as ex:
        logging.error(ex)
        raise gen.Return()

    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    client = Calculator.Client(transport, pfactory)

    # ping
    yield client.ping()
    print("ping()")

    # add
    sum_ = yield client.add(1, 1)
    print("1 + 1 = {0}".format(sum_))

    # make a oneway call without a callback (schedule the write and continue
    # without blocking)
    client.zip()
    print("zip() without callback")

    # make a oneway call with a callback (we'll wait for the stream write to
    # complete before continuing)
    client.zip()
    print("zip() with callback")

    # calculate 1/0
    work = Work()
    work.op = Operation.DIVIDE
    work.num1 = 1
    work.num2 = 0

    try:
        quotient = yield client.calculate(1, work)
        print("Whoa? You know how to divide by zero ? -> {0}".format(quotient))
    except InvalidOperation as io:
        print("InvalidOperation: {0}".format(io))

    # calculate 15-10
    work.op = Operation.SUBTRACT
    work.num1 = 15
    work.num2 = 10

    diff = yield client.calculate(1, work)
    print("15 - 10 = {0}".format(diff))

    # getStruct
    log = yield client.getStruct(1)
    print("Check log: {0}".format(log.value))

    # close the transport
    client._transport.close()
    raise gen.Return()


def main():
    # create an ioloop, do the above, then stop
    ioloop.IOLoop.current().run_sync(communicate)


if __name__ == "__main__":
    main()
