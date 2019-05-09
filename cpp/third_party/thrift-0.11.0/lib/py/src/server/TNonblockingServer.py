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
"""Implementation of non-blocking server.

The main idea of the server is to receive and send requests
only from the main thread.

The thread poool should be sized for concurrent tasks, not
maximum connections
"""

import logging
import select
import socket
import struct
import threading

from collections import deque
from six.moves import queue

from thrift.transport import TTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocolFactory

__all__ = ['TNonblockingServer']

logger = logging.getLogger(__name__)


class Worker(threading.Thread):
    """Worker is a small helper to process incoming connection."""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        """Process queries from task queue, stop if processor is None."""
        while True:
            try:
                processor, iprot, oprot, otrans, callback = self.queue.get()
                if processor is None:
                    break
                processor.process(iprot, oprot)
                callback(True, otrans.getvalue())
            except Exception:
                logger.exception("Exception while processing request", exc_info=True)
                callback(False, b'')


WAIT_LEN = 0
WAIT_MESSAGE = 1
WAIT_PROCESS = 2
SEND_ANSWER = 3
CLOSED = 4


def locked(func):
    """Decorator which locks self.lock."""
    def nested(self, *args, **kwargs):
        self.lock.acquire()
        try:
            return func(self, *args, **kwargs)
        finally:
            self.lock.release()
    return nested


def socket_exception(func):
    """Decorator close object on socket.error."""
    def read(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except socket.error:
            logger.debug('ignoring socket exception', exc_info=True)
            self.close()
    return read


class Message(object):
    def __init__(self, offset, len_, header):
        self.offset = offset
        self.len = len_
        self.buffer = None
        self.is_header = header

    @property
    def end(self):
        return self.offset + self.len


class Connection(object):
    """Basic class is represented connection.

    It can be in state:
        WAIT_LEN --- connection is reading request len.
        WAIT_MESSAGE --- connection is reading request.
        WAIT_PROCESS --- connection has just read whole request and
                         waits for call ready routine.
        SEND_ANSWER --- connection is sending answer string (including length
                        of answer).
        CLOSED --- socket was closed and connection should be deleted.
    """
    def __init__(self, new_socket, wake_up):
        self.socket = new_socket
        self.socket.setblocking(False)
        self.status = WAIT_LEN
        self.len = 0
        self.received = deque()
        self._reading = Message(0, 4, True)
        self._rbuf = b''
        self._wbuf = b''
        self.lock = threading.Lock()
        self.wake_up = wake_up
        self.remaining = False

    @socket_exception
    def read(self):
        """Reads data from stream and switch state."""
        assert self.status in (WAIT_LEN, WAIT_MESSAGE)
        assert not self.received
        buf_size = 8192
        first = True
        done = False
        while not done:
            read = self.socket.recv(buf_size)
            rlen = len(read)
            done = rlen < buf_size
            self._rbuf += read
            if first and rlen == 0:
                if self.status != WAIT_LEN or self._rbuf:
                    logger.error('could not read frame from socket')
                else:
                    logger.debug('read zero length. client might have disconnected')
                self.close()
            while len(self._rbuf) >= self._reading.end:
                if self._reading.is_header:
                    mlen, = struct.unpack('!i', self._rbuf[:4])
                    self._reading = Message(self._reading.end, mlen, False)
                    self.status = WAIT_MESSAGE
                else:
                    self._reading.buffer = self._rbuf
                    self.received.append(self._reading)
                    self._rbuf = self._rbuf[self._reading.end:]
                    self._reading = Message(0, 4, True)
            first = False
            if self.received:
                self.status = WAIT_PROCESS
                break
        self.remaining = not done

    @socket_exception
    def write(self):
        """Writes data from socket and switch state."""
        assert self.status == SEND_ANSWER
        sent = self.socket.send(self._wbuf)
        if sent == len(self._wbuf):
            self.status = WAIT_LEN
            self._wbuf = b''
            self.len = 0
        else:
            self._wbuf = self.message[sent:]

    @locked
    def ready(self, all_ok, message):
        """Callback function for switching state and waking up main thread.

        This function is the only function witch can be called asynchronous.

        The ready can switch Connection to three states:
            WAIT_LEN if request was oneway.
            SEND_ANSWER if request was processed in normal way.
            CLOSED if request throws unexpected exception.

        The one wakes up main thread.
        """
        assert self.status == WAIT_PROCESS
        if not all_ok:
            self.close()
            self.wake_up()
            return
        self.len = 0
        if len(message) == 0:
            # it was a oneway request, do not write answer
            self._wbuf = b''
            self.status = WAIT_LEN
        else:
            self._wbuf = struct.pack('!i', len(message)) + message
            self.status = SEND_ANSWER
        self.wake_up()

    @locked
    def is_writeable(self):
        """Return True if connection should be added to write list of select"""
        return self.status == SEND_ANSWER

    # it's not necessary, but...
    @locked
    def is_readable(self):
        """Return True if connection should be added to read list of select"""
        return self.status in (WAIT_LEN, WAIT_MESSAGE)

    @locked
    def is_closed(self):
        """Returns True if connection is closed."""
        return self.status == CLOSED

    def fileno(self):
        """Returns the file descriptor of the associated socket."""
        return self.socket.fileno()

    def close(self):
        """Closes connection"""
        self.status = CLOSED
        self.socket.close()


class TNonblockingServer(object):
    """Non-blocking server."""

    def __init__(self,
                 processor,
                 lsocket,
                 inputProtocolFactory=None,
                 outputProtocolFactory=None,
                 threads=10):
        self.processor = processor
        self.socket = lsocket
        self.in_protocol = inputProtocolFactory or TBinaryProtocolFactory()
        self.out_protocol = outputProtocolFactory or self.in_protocol
        self.threads = int(threads)
        self.clients = {}
        self.tasks = queue.Queue()
        self._read, self._write = socket.socketpair()
        self.prepared = False
        self._stop = False

    def setNumThreads(self, num):
        """Set the number of worker threads that should be created."""
        # implement ThreadPool interface
        assert not self.prepared, "Can't change number of threads after start"
        self.threads = num

    def prepare(self):
        """Prepares server for serve requests."""
        if self.prepared:
            return
        self.socket.listen()
        for _ in range(self.threads):
            thread = Worker(self.tasks)
            thread.setDaemon(True)
            thread.start()
        self.prepared = True

    def wake_up(self):
        """Wake up main thread.

        The server usually waits in select call in we should terminate one.
        The simplest way is using socketpair.

        Select always wait to read from the first socket of socketpair.

        In this case, we can just write anything to the second socket from
        socketpair.
        """
        self._write.send(b'1')

    def stop(self):
        """Stop the server.

        This method causes the serve() method to return.  stop() may be invoked
        from within your handler, or from another thread.

        After stop() is called, serve() will return but the server will still
        be listening on the socket.  serve() may then be called again to resume
        processing requests.  Alternatively, close() may be called after
        serve() returns to close the server socket and shutdown all worker
        threads.
        """
        self._stop = True
        self.wake_up()

    def _select(self):
        """Does select on open connections."""
        readable = [self.socket.handle.fileno(), self._read.fileno()]
        writable = []
        remaining = []
        for i, connection in list(self.clients.items()):
            if connection.is_readable():
                readable.append(connection.fileno())
                if connection.remaining or connection.received:
                    remaining.append(connection.fileno())
            if connection.is_writeable():
                writable.append(connection.fileno())
            if connection.is_closed():
                del self.clients[i]
        if remaining:
            return remaining, [], [], False
        else:
            return select.select(readable, writable, readable) + (True,)

    def handle(self):
        """Handle requests.

        WARNING! You must call prepare() BEFORE calling handle()
        """
        assert self.prepared, "You have to call prepare before handle"
        rset, wset, xset, selected = self._select()
        for readable in rset:
            if readable == self._read.fileno():
                # don't care i just need to clean readable flag
                self._read.recv(1024)
            elif readable == self.socket.handle.fileno():
                try:
                    client = self.socket.accept()
                    if client:
                        self.clients[client.handle.fileno()] = Connection(client.handle,
                                                                          self.wake_up)
                except socket.error:
                    logger.debug('error while accepting', exc_info=True)
            else:
                connection = self.clients[readable]
                if selected:
                    connection.read()
                if connection.received:
                    connection.status = WAIT_PROCESS
                    msg = connection.received.popleft()
                    itransport = TTransport.TMemoryBuffer(msg.buffer, msg.offset)
                    otransport = TTransport.TMemoryBuffer()
                    iprot = self.in_protocol.getProtocol(itransport)
                    oprot = self.out_protocol.getProtocol(otransport)
                    self.tasks.put([self.processor, iprot, oprot,
                                    otransport, connection.ready])
        for writeable in wset:
            self.clients[writeable].write()
        for oob in xset:
            self.clients[oob].close()
            del self.clients[oob]

    def close(self):
        """Closes the server."""
        for _ in range(self.threads):
            self.tasks.put([None, None, None, None, None])
        self.socket.close()
        self.prepared = False

    def serve(self):
        """Serve requests.

        Serve requests forever, or until stop() is called.
        """
        self._stop = False
        self.prepare()
        while not self._stop:
            self.handle()
