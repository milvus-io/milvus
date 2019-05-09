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
import zmq
from cStringIO import StringIO
from thrift.transport.TTransport import TTransportBase, CReadableTransport


class TZmqClient(TTransportBase, CReadableTransport):
    def __init__(self, ctx, endpoint, sock_type):
        self._sock = ctx.socket(sock_type)
        self._endpoint = endpoint
        self._wbuf = StringIO()
        self._rbuf = StringIO()

    def open(self):
        self._sock.connect(self._endpoint)

    def read(self, size):
        ret = self._rbuf.read(size)
        if len(ret) != 0:
            return ret
        self._read_message()
        return self._rbuf.read(size)

    def _read_message(self):
        msg = self._sock.recv()
        self._rbuf = StringIO(msg)

    def write(self, buf):
        self._wbuf.write(buf)

    def flush(self):
        msg = self._wbuf.getvalue()
        self._wbuf = StringIO()
        self._sock.send(msg)

    # Implement the CReadableTransport interface.
    @property
    def cstringio_buf(self):
        return self._rbuf

    # NOTE: This will probably not actually work.
    def cstringio_refill(self, prefix, reqlen):
        while len(prefix) < reqlen:
            self.read_message()
            prefix += self._rbuf.getvalue()
        self._rbuf = StringIO(prefix)
        return self._rbuf
