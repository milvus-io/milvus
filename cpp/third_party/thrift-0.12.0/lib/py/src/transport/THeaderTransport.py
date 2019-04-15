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

import struct
import zlib

from thrift.compat import BufferIO, byte_index
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol, readVarint, writeVarint
from thrift.Thrift import TApplicationException
from thrift.transport.TTransport import (
    CReadableTransport,
    TMemoryBuffer,
    TTransportBase,
    TTransportException,
)


U16 = struct.Struct("!H")
I32 = struct.Struct("!i")
HEADER_MAGIC = 0x0FFF
HARD_MAX_FRAME_SIZE = 0x3FFFFFFF


class THeaderClientType(object):
    HEADERS = 0x00

    FRAMED_BINARY = 0x01
    UNFRAMED_BINARY = 0x02

    FRAMED_COMPACT = 0x03
    UNFRAMED_COMPACT = 0x04


class THeaderSubprotocolID(object):
    BINARY = 0x00
    COMPACT = 0x02


class TInfoHeaderType(object):
    KEY_VALUE = 0x01


class THeaderTransformID(object):
    ZLIB = 0x01


READ_TRANSFORMS_BY_ID = {
    THeaderTransformID.ZLIB: zlib.decompress,
}


WRITE_TRANSFORMS_BY_ID = {
    THeaderTransformID.ZLIB: zlib.compress,
}


def _readString(trans):
    size = readVarint(trans)
    if size < 0:
        raise TTransportException(
            TTransportException.NEGATIVE_SIZE,
            "Negative length"
        )
    return trans.read(size)


def _writeString(trans, value):
    writeVarint(trans, len(value))
    trans.write(value)


class THeaderTransport(TTransportBase, CReadableTransport):
    def __init__(self, transport, allowed_client_types):
        self._transport = transport
        self._client_type = THeaderClientType.HEADERS
        self._allowed_client_types = allowed_client_types

        self._read_buffer = BufferIO(b"")
        self._read_headers = {}

        self._write_buffer = BufferIO()
        self._write_headers = {}
        self._write_transforms = []

        self.flags = 0
        self.sequence_id = 0
        self._protocol_id = THeaderSubprotocolID.BINARY
        self._max_frame_size = HARD_MAX_FRAME_SIZE

    def isOpen(self):
        return self._transport.isOpen()

    def open(self):
        return self._transport.open()

    def close(self):
        return self._transport.close()

    def get_headers(self):
        return self._read_headers

    def set_header(self, key, value):
        if not isinstance(key, bytes):
            raise ValueError("header names must be bytes")
        if not isinstance(value, bytes):
            raise ValueError("header values must be bytes")
        self._write_headers[key] = value

    def clear_headers(self):
        self._write_headers.clear()

    def add_transform(self, transform_id):
        if transform_id not in WRITE_TRANSFORMS_BY_ID:
            raise ValueError("unknown transform")
        self._write_transforms.append(transform_id)

    def set_max_frame_size(self, size):
        if not 0 < size < HARD_MAX_FRAME_SIZE:
            raise ValueError("maximum frame size should be < %d and > 0" % HARD_MAX_FRAME_SIZE)
        self._max_frame_size = size

    @property
    def protocol_id(self):
        if self._client_type == THeaderClientType.HEADERS:
            return self._protocol_id
        elif self._client_type in (THeaderClientType.FRAMED_BINARY, THeaderClientType.UNFRAMED_BINARY):
            return THeaderSubprotocolID.BINARY
        elif self._client_type in (THeaderClientType.FRAMED_COMPACT, THeaderClientType.UNFRAMED_COMPACT):
            return THeaderSubprotocolID.COMPACT
        else:
            raise TTransportException(
                TTransportException.INVALID_CLIENT_TYPE,
                "Protocol ID not know for client type %d" % self._client_type,
            )

    def read(self, sz):
        # if there are bytes left in the buffer, produce those first.
        bytes_read = self._read_buffer.read(sz)
        bytes_left_to_read = sz - len(bytes_read)
        if bytes_left_to_read == 0:
            return bytes_read

        # if we've determined this is an unframed client, just pass the read
        # through to the underlying transport until we're reset again at the
        # beginning of the next message.
        if self._client_type in (THeaderClientType.UNFRAMED_BINARY, THeaderClientType.UNFRAMED_COMPACT):
            return bytes_read + self._transport.read(bytes_left_to_read)

        # we're empty and (maybe) framed. fill the buffers with the next frame.
        self.readFrame(bytes_left_to_read)
        return bytes_read + self._read_buffer.read(bytes_left_to_read)

    def _set_client_type(self, client_type):
        if client_type not in self._allowed_client_types:
            raise TTransportException(
                TTransportException.INVALID_CLIENT_TYPE,
                "Client type %d not allowed by server." % client_type,
            )
        self._client_type = client_type

    def readFrame(self, req_sz):
        # the first word could either be the length field of a framed message
        # or the first bytes of an unframed message.
        first_word = self._transport.readAll(I32.size)
        frame_size, = I32.unpack(first_word)
        is_unframed = False
        if frame_size & TBinaryProtocol.VERSION_MASK == TBinaryProtocol.VERSION_1:
            self._set_client_type(THeaderClientType.UNFRAMED_BINARY)
            is_unframed = True
        elif (byte_index(first_word, 0) == TCompactProtocol.PROTOCOL_ID and
              byte_index(first_word, 1) & TCompactProtocol.VERSION_MASK == TCompactProtocol.VERSION):
            self._set_client_type(THeaderClientType.UNFRAMED_COMPACT)
            is_unframed = True

        if is_unframed:
            bytes_left_to_read = req_sz - I32.size
            if bytes_left_to_read > 0:
                rest = self._transport.read(bytes_left_to_read)
            else:
                rest = b""
            self._read_buffer = BufferIO(first_word + rest)
            return

        # ok, we're still here so we're framed.
        if frame_size > self._max_frame_size:
            raise TTransportException(
                TTransportException.SIZE_LIMIT,
                "Frame was too large.",
            )
        read_buffer = BufferIO(self._transport.readAll(frame_size))

        # the next word is either going to be the version field of a
        # binary/compact protocol message or the magic value + flags of a
        # header protocol message.
        second_word = read_buffer.read(I32.size)
        version, = I32.unpack(second_word)
        read_buffer.seek(0)
        if version >> 16 == HEADER_MAGIC:
            self._set_client_type(THeaderClientType.HEADERS)
            self._read_buffer = self._parse_header_format(read_buffer)
        elif version & TBinaryProtocol.VERSION_MASK == TBinaryProtocol.VERSION_1:
            self._set_client_type(THeaderClientType.FRAMED_BINARY)
            self._read_buffer = read_buffer
        elif (byte_index(second_word, 0) == TCompactProtocol.PROTOCOL_ID and
              byte_index(second_word, 1) & TCompactProtocol.VERSION_MASK == TCompactProtocol.VERSION):
            self._set_client_type(THeaderClientType.FRAMED_COMPACT)
            self._read_buffer = read_buffer
        else:
            raise TTransportException(
                TTransportException.INVALID_CLIENT_TYPE,
                "Could not detect client transport type.",
            )

    def _parse_header_format(self, buffer):
        # make BufferIO look like TTransport for varint helpers
        buffer_transport = TMemoryBuffer()
        buffer_transport._buffer = buffer

        buffer.read(2)  # discard the magic bytes
        self.flags, = U16.unpack(buffer.read(U16.size))
        self.sequence_id, = I32.unpack(buffer.read(I32.size))

        header_length = U16.unpack(buffer.read(U16.size))[0] * 4
        end_of_headers = buffer.tell() + header_length
        if end_of_headers > len(buffer.getvalue()):
            raise TTransportException(
                TTransportException.SIZE_LIMIT,
                "Header size is larger than whole frame.",
            )

        self._protocol_id = readVarint(buffer_transport)

        transforms = []
        transform_count = readVarint(buffer_transport)
        for _ in range(transform_count):
            transform_id = readVarint(buffer_transport)
            if transform_id not in READ_TRANSFORMS_BY_ID:
                raise TApplicationException(
                    TApplicationException.INVALID_TRANSFORM,
                    "Unknown transform: %d" % transform_id,
                )
            transforms.append(transform_id)
        transforms.reverse()

        headers = {}
        while buffer.tell() < end_of_headers:
            header_type = readVarint(buffer_transport)
            if header_type == TInfoHeaderType.KEY_VALUE:
                count = readVarint(buffer_transport)
                for _ in range(count):
                    key = _readString(buffer_transport)
                    value = _readString(buffer_transport)
                    headers[key] = value
            else:
                break  # ignore unknown headers
        self._read_headers = headers

        # skip padding / anything we didn't understand
        buffer.seek(end_of_headers)

        payload = buffer.read()
        for transform_id in transforms:
            transform_fn = READ_TRANSFORMS_BY_ID[transform_id]
            payload = transform_fn(payload)
        return BufferIO(payload)

    def write(self, buf):
        self._write_buffer.write(buf)

    def flush(self):
        payload = self._write_buffer.getvalue()
        self._write_buffer = BufferIO()

        buffer = BufferIO()
        if self._client_type == THeaderClientType.HEADERS:
            for transform_id in self._write_transforms:
                transform_fn = WRITE_TRANSFORMS_BY_ID[transform_id]
                payload = transform_fn(payload)

            headers = BufferIO()
            writeVarint(headers, self._protocol_id)
            writeVarint(headers, len(self._write_transforms))
            for transform_id in self._write_transforms:
                writeVarint(headers, transform_id)
            if self._write_headers:
                writeVarint(headers, TInfoHeaderType.KEY_VALUE)
                writeVarint(headers, len(self._write_headers))
                for key, value in self._write_headers.items():
                    _writeString(headers, key)
                    _writeString(headers, value)
                self._write_headers = {}
            padding_needed = (4 - (len(headers.getvalue()) % 4)) % 4
            headers.write(b"\x00" * padding_needed)
            header_bytes = headers.getvalue()

            buffer.write(I32.pack(10 + len(header_bytes) + len(payload)))
            buffer.write(U16.pack(HEADER_MAGIC))
            buffer.write(U16.pack(self.flags))
            buffer.write(I32.pack(self.sequence_id))
            buffer.write(U16.pack(len(header_bytes) // 4))
            buffer.write(header_bytes)
            buffer.write(payload)
        elif self._client_type in (THeaderClientType.FRAMED_BINARY, THeaderClientType.FRAMED_COMPACT):
            buffer.write(I32.pack(len(payload)))
            buffer.write(payload)
        elif self._client_type in (THeaderClientType.UNFRAMED_BINARY, THeaderClientType.UNFRAMED_COMPACT):
            buffer.write(payload)
        else:
            raise TTransportException(
                TTransportException.INVALID_CLIENT_TYPE,
                "Unknown client type.",
            )

        # the frame length field doesn't count towards the frame payload size
        frame_bytes = buffer.getvalue()
        frame_payload_size = len(frame_bytes) - 4
        if frame_payload_size > self._max_frame_size:
            raise TTransportException(
                TTransportException.SIZE_LIMIT,
                "Attempting to send frame that is too large.",
            )

        self._transport.write(frame_bytes)
        self._transport.flush()

    @property
    def cstringio_buf(self):
        return self._read_buffer

    def cstringio_refill(self, partialread, reqlen):
        result = bytearray(partialread)
        while len(result) < reqlen:
            result += self.read(reqlen - len(result))
        self._read_buffer = BufferIO(result)
        return self._read_buffer
