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

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.protocol.TCompactProtocol import TCompactProtocolAccelerated
from thrift.protocol.TProtocol import TProtocolBase, TProtocolException
from thrift.Thrift import TApplicationException, TMessageType
from thrift.transport.THeaderTransport import THeaderTransport, THeaderSubprotocolID, THeaderClientType


PROTOCOLS_BY_ID = {
    THeaderSubprotocolID.BINARY: TBinaryProtocolAccelerated,
    THeaderSubprotocolID.COMPACT: TCompactProtocolAccelerated,
}


class THeaderProtocol(TProtocolBase):
    """A framed protocol with headers and payload transforms.

    THeaderProtocol frames other Thrift protocols and adds support for optional
    out-of-band headers. The currently supported subprotocols are
    TBinaryProtocol and TCompactProtocol.

    It's also possible to apply transforms to the encoded message payload. The
    only transform currently supported is to gzip.

    When used in a server, THeaderProtocol can accept messages from
    non-THeaderProtocol clients if allowed (see `allowed_client_types`). This
    includes framed and unframed transports and both TBinaryProtocol and
    TCompactProtocol. The server will respond in the appropriate dialect for
    the connected client. HTTP clients are not currently supported.

    THeaderProtocol does not currently support THTTPServer, TNonblockingServer,
    or TProcessPoolServer.

    See doc/specs/HeaderFormat.md for details of the wire format.

    """

    def __init__(self, transport, allowed_client_types):
        # much of the actual work for THeaderProtocol happens down in
        # THeaderTransport since we need to do low-level shenanigans to detect
        # if the client is sending us headers or one of the headerless formats
        # we support. this wraps the real transport with the one that does all
        # the magic.
        if not isinstance(transport, THeaderTransport):
            transport = THeaderTransport(transport, allowed_client_types)
        super(THeaderProtocol, self).__init__(transport)
        self._set_protocol()

    def get_headers(self):
        return self.trans.get_headers()

    def set_header(self, key, value):
        self.trans.set_header(key, value)

    def clear_headers(self):
        self.trans.clear_headers()

    def add_transform(self, transform_id):
        self.trans.add_transform(transform_id)

    def writeMessageBegin(self, name, ttype, seqid):
        self.trans.sequence_id = seqid
        return self._protocol.writeMessageBegin(name, ttype, seqid)

    def writeMessageEnd(self):
        return self._protocol.writeMessageEnd()

    def writeStructBegin(self, name):
        return self._protocol.writeStructBegin(name)

    def writeStructEnd(self):
        return self._protocol.writeStructEnd()

    def writeFieldBegin(self, name, ttype, fid):
        return self._protocol.writeFieldBegin(name, ttype, fid)

    def writeFieldEnd(self):
        return self._protocol.writeFieldEnd()

    def writeFieldStop(self):
        return self._protocol.writeFieldStop()

    def writeMapBegin(self, ktype, vtype, size):
        return self._protocol.writeMapBegin(ktype, vtype, size)

    def writeMapEnd(self):
        return self._protocol.writeMapEnd()

    def writeListBegin(self, etype, size):
        return self._protocol.writeListBegin(etype, size)

    def writeListEnd(self):
        return self._protocol.writeListEnd()

    def writeSetBegin(self, etype, size):
        return self._protocol.writeSetBegin(etype, size)

    def writeSetEnd(self):
        return self._protocol.writeSetEnd()

    def writeBool(self, bool_val):
        return self._protocol.writeBool(bool_val)

    def writeByte(self, byte):
        return self._protocol.writeByte(byte)

    def writeI16(self, i16):
        return self._protocol.writeI16(i16)

    def writeI32(self, i32):
        return self._protocol.writeI32(i32)

    def writeI64(self, i64):
        return self._protocol.writeI64(i64)

    def writeDouble(self, dub):
        return self._protocol.writeDouble(dub)

    def writeBinary(self, str_val):
        return self._protocol.writeBinary(str_val)

    def _set_protocol(self):
        try:
            protocol_cls = PROTOCOLS_BY_ID[self.trans.protocol_id]
        except KeyError:
            raise TApplicationException(
                TProtocolException.INVALID_PROTOCOL,
                "Unknown protocol requested.",
            )

        self._protocol = protocol_cls(self.trans)
        self._fast_encode = self._protocol._fast_encode
        self._fast_decode = self._protocol._fast_decode

    def readMessageBegin(self):
        try:
            self.trans.readFrame(0)
            self._set_protocol()
        except TApplicationException as exc:
            self._protocol.writeMessageBegin(b"", TMessageType.EXCEPTION, 0)
            exc.write(self._protocol)
            self._protocol.writeMessageEnd()
            self.trans.flush()

        return self._protocol.readMessageBegin()

    def readMessageEnd(self):
        return self._protocol.readMessageEnd()

    def readStructBegin(self):
        return self._protocol.readStructBegin()

    def readStructEnd(self):
        return self._protocol.readStructEnd()

    def readFieldBegin(self):
        return self._protocol.readFieldBegin()

    def readFieldEnd(self):
        return self._protocol.readFieldEnd()

    def readMapBegin(self):
        return self._protocol.readMapBegin()

    def readMapEnd(self):
        return self._protocol.readMapEnd()

    def readListBegin(self):
        return self._protocol.readListBegin()

    def readListEnd(self):
        return self._protocol.readListEnd()

    def readSetBegin(self):
        return self._protocol.readSetBegin()

    def readSetEnd(self):
        return self._protocol.readSetEnd()

    def readBool(self):
        return self._protocol.readBool()

    def readByte(self):
        return self._protocol.readByte()

    def readI16(self):
        return self._protocol.readI16()

    def readI32(self):
        return self._protocol.readI32()

    def readI64(self):
        return self._protocol.readI64()

    def readDouble(self):
        return self._protocol.readDouble()

    def readBinary(self):
        return self._protocol.readBinary()


class THeaderProtocolFactory(object):
    def __init__(self, allowed_client_types=(THeaderClientType.HEADERS,)):
        self.allowed_client_types = allowed_client_types

    def getProtocol(self, trans):
        return THeaderProtocol(trans, self.allowed_client_types)
