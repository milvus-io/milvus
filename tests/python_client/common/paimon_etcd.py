"""Minimal fixed-protocol etcd KV access for the opt-in L3 driver only.

Field numbers follow etcd api/v3 etcdserverpb/rpc.proto and mvccpb/kv.proto.
This avoids installing another etcd client into the ordinary Python test image.
"""

from urllib.parse import urlparse

import grpc


def varint(value):
    if value < 0 or value >= 2**64:
        raise ValueError("protobuf uint64 outside range")
    output = bytearray()
    while value >= 128:
        output.append((value & 127) | 128)
        value >>= 7
    output.append(value)
    return bytes(output)


def _read_varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise ValueError("truncated protobuf varint")
        byte = data[offset]
        offset += 1
        if shift == 63 and byte > 1:
            raise ValueError("protobuf varint overflow")
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise ValueError("invalid protobuf varint")


def integer_field(number, value):
    return varint(number << 3) + varint(value)


def bytes_field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value


def decode_fields(data):
    if len(data) > 16 * 1024 * 1024:
        raise ValueError("protobuf message too large")
    fields, offset = {}, 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        number, wire = tag >> 3, tag & 7
        if not number:
            raise ValueError("protobuf field zero")
        if wire == 0:
            value, offset = _read_varint(data, offset)
        elif wire in {1, 2, 5}:
            if wire == 2:
                length, offset = _read_varint(data, offset)
            else:
                length = 8 if wire == 1 else 4
            end = offset + length
            if end > len(data):
                raise ValueError("truncated protobuf field")
            value, offset = data[offset:end], end
        else:
            raise ValueError(f"unsupported protobuf wire type: {wire}")
        fields.setdefault(number, []).append(value)
    return fields


def scalar(fields, number, default=0):
    values = fields.get(number, [])
    return values[-1] if values else default


class EtcdKV:
    def __init__(self, endpoint):
        url = urlparse(endpoint)
        if (
            url.scheme != "http"
            or not url.hostname
            or url.username
            or url.password
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
        ):
            raise ValueError("L3 cell driver requires its dedicated plaintext etcd endpoint")
        self.channel = grpc.insecure_channel(url.netloc)

    def _call(self, method, request):
        rpc = self.channel.unary_unary(
            "/etcdserverpb.KV/" + method, request_serializer=lambda value: value, response_deserializer=decode_fields
        )
        return rpc(request, timeout=10)

    def range(self, key, prefix=False):
        key = key.encode()
        request = bytes_field(1, key)
        if prefix:
            end = bytearray(key)
            while end and end[-1] == 255:
                end.pop()
            if end:
                end[-1] += 1
            request += bytes_field(2, bytes(end) if end else b"\x00")
        response = self._call("Range", request)
        if scalar(response, 3, 0):
            raise ValueError("etcd range was truncated; evidence is incomplete")
        rows = []
        for entry in response.get(2, []):
            fields = decode_fields(entry)
            rows.append((scalar(fields, 1, b"").decode(), scalar(fields, 5, b"")))
        return rows

    def put(self, key, value):
        self._call("Put", bytes_field(1, key.encode()) + bytes_field(2, value.encode()))

    def delete(self, key):
        self._call("DeleteRange", bytes_field(1, key.encode()))

    def acquire(self, key, owner):
        # Compare VERSION == 0, then atomically put this owner.
        compare = bytes_field(3, key.encode()) + integer_field(4, 0)
        put = bytes_field(1, key.encode()) + bytes_field(2, owner.encode())
        request = bytes_field(1, compare) + bytes_field(2, bytes_field(2, put))
        return bool(scalar(self._call("Txn", request), 2))

    def release(self, key, owner):
        # Compare VALUE == owner, then delete exactly this key.
        compare = integer_field(2, 3) + bytes_field(3, key.encode()) + bytes_field(7, owner.encode())
        request = bytes_field(1, compare) + bytes_field(2, bytes_field(3, bytes_field(1, key.encode())))
        return bool(scalar(self._call("Txn", request), 2))

    def close(self):
        self.channel.close()
