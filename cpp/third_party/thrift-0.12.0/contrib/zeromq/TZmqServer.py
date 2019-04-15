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
import logging
import zmq
import thrift.server.TServer
import thrift.transport.TTransport


class TZmqServer(thrift.server.TServer.TServer):
    def __init__(self, processor, ctx, endpoint, sock_type):
        thrift.server.TServer.TServer.__init__(self, processor, None)
        self.zmq_type = sock_type
        self.socket = ctx.socket(sock_type)
        self.socket.bind(endpoint)

    def serveOne(self):
        msg = self.socket.recv()
        itrans = thrift.transport.TTransport.TMemoryBuffer(msg)
        otrans = thrift.transport.TTransport.TMemoryBuffer()
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)

        try:
            self.processor.process(iprot, oprot)
        except Exception:
            logging.exception("Exception while processing request")
            # Fall through and send back a response, even if empty or incomplete.

        if self.zmq_type == zmq.REP:
            msg = otrans.getvalue()
            self.socket.send(msg)

    def serve(self):
        while True:
            self.serveOne()


class TZmqMultiServer(object):
    def __init__(self):
        self.servers = []

    def serveOne(self, timeout=-1):
        self._serveActive(self._setupPoll(), timeout)

    def serveForever(self):
        poll_info = self._setupPoll()
        while True:
            self._serveActive(poll_info, -1)

    def _setupPoll(self):
        server_map = {}
        poller = zmq.Poller()
        for server in self.servers:
            server_map[server.socket] = server
            poller.register(server.socket, zmq.POLLIN)
        return (server_map, poller)

    def _serveActive(self, poll_info, timeout):
        (server_map, poller) = poll_info
        ready = dict(poller.poll())
        for sock, state in ready.items():
            assert (state & zmq.POLLIN) != 0
            server_map[sock].serveOne()
