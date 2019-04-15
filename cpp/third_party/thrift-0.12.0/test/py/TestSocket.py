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

from thrift.transport import TSocket
import unittest
import time
import socket
import random


class TimeoutTest(unittest.TestCase):
    def setUp(self):
        for i in range(50):
            try:
                # find a port we can use
                self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.port = random.randint(10000, 30000)
                self.listen_sock.bind(('localhost', self.port))
                self.listen_sock.listen(5)
                break
            except Exception:
                if i == 49:
                    raise

    def testConnectTimeout(self):
        starttime = time.time()

        try:
            leaky = []
            for i in range(100):
                socket = TSocket.TSocket('localhost', self.port)
                socket.setTimeout(10)
                socket.open()
                leaky.append(socket)
        except Exception:
            self.assert_(time.time() - starttime < 5.0)

    def testWriteTimeout(self):
        starttime = time.time()

        try:
            socket = TSocket.TSocket('localhost', self.port)
            socket.setTimeout(10)
            socket.open()
            lsock = self.listen_sock.accept()
            while True:
                lsock.write("hi" * 100)

        except Exception:
            self.assert_(time.time() - starttime < 5.0)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    suite.addTest(loader.loadTestsFromTestCase(TimeoutTest))

    testRunner = unittest.TextTestRunner(verbosity=2)
    testRunner.run(suite)
