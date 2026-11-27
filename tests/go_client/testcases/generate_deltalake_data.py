#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Generate deterministic Delta logs and Parquet; no checked-in data files.

Versions: 0 -> rows 0..5; 1 -> rows 0..9; 2 -> rows 6..9; 3 -> empty.
Historical objects remain available for pinned reads.
"""
import json
import sys
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def field_type_columns():
    def array_of(element_type):
        return {"type": "array", "elementType": element_type, "containsNull": False}

    return [
        ("flag", "boolean", pa.bool_(), True),
        ("tiny", "byte", pa.int8(), -8),
        ("small", "short", pa.int16(), -160),
        ("number", "integer", pa.int32(), 320),
        ("single", "float", pa.float32(), 1.25),
        ("double_value", "double", pa.float64(), 2.5),
        ("body", "string", pa.string(), "Delta text"),
        ("attributes", "string", pa.string(), '{"score":7}'),
        ("numbers", array_of("integer"), pa.list_(pa.int32()), [2, -3]),
        ("event_time", "timestamp", pa.timestamp("us", tz="UTC"), 1704067200123456),
        ("point", "string", pa.string(), "POINT (1 2)"),
        # Delta binary preserves the native encoding of half and packed vectors.
        ("fp16", "binary", pa.binary(), struct.pack("<4e", 1, 2, 3, 4)),
        ("bf16", "binary", pa.binary(), struct.pack("<4H", 0x3F80, 0x4000, 0x4040, 0x4080)),
        ("bits", "binary", pa.binary(), bytes([0xA5, 0xC3])),
        ("int8_vector", array_of("byte"), pa.list_(pa.int8()), [-1, 2, -3, 4]),
        ("sparse", {"type": "map", "keyType": "integer", "valueType": "float", "valueContainsNull": False},
         pa.map_(pa.int32(), pa.float32()), [(7, 0.25), (2, 0.5)]),
    ]


def generate(root, deletion_vectors=False, column_mapping=False, partitioned=False, field_types=False):
    root.mkdir(parents=True, exist_ok=True)
    log = root / "_delta_log"
    log.mkdir()
    fields = [
        {"name": "pk", "type": "long", "nullable": False, "metadata": {}},
        {"name": "label", "type": "string", "nullable": True, "metadata": {}},
        {"name": "vector", "type": {"type": "array", "elementType": "float", "containsNull": False},
         "nullable": False, "metadata": {}},
    ]
    extra_columns = field_type_columns() if field_types else []
    fields.extend({"name": name, "type": delta_type, "nullable": True, "metadata": {}}
                  for name, delta_type, _, _ in extra_columns)
    metadata = {"id": "00000000-0000-0000-0000-000000000001",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [], "configuration": {}, "createdTime": 0}

    if column_mapping:
        metadata["configuration"] = {"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "3"}
        for index, field in enumerate(fields, 1):
            field["metadata"] = {"delta.columnMapping.id": index,
                                 "delta.columnMapping.physicalName": f"physical_{field['name']}"}
        metadata["schemaString"] = json.dumps({"type": "struct", "fields": fields})
    if partitioned:
        metadata["partitionColumns"] = ["label"]

    def add(name, begin, end):
        table = pa.table({"pk": pa.array(range(begin, end), type=pa.int64()),
                          "label": [f"label_{i}" for i in range(begin, end)],
                          "vector": pa.array([[float(i + 1), 1., 2., 3.] for i in range(begin, end)],
                                             type=pa.list_(pa.float32()))})
        for column_name, _, arrow_type, value in extra_columns:
            table = table.append_column(column_name, pa.array([value] * (end - begin), type=arrow_type))
        partitions = {}
        if partitioned:
            partitions["label"] = "partition_label"
            table = table.drop(["label"])
        if column_mapping:
            table = table.rename_columns([f"physical_{name}" for name in table.column_names])
        pq.write_table(table, root / name, row_group_size=2)
        return {"add": {"path": name, "partitionValues": partitions, "size": (root / name).stat().st_size,
                        "modificationTime": 0, "dataChange": True,
                        "stats": json.dumps({"numRecords": end - begin})}}

    def remove(name):
        return {"remove": {"path": name, "deletionTimestamp": 0, "dataChange": True}}

    commits = [
        [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}, {"metaData": metadata}, add("first.parquet", 0, 6)],
        [add("second.parquet", 6, 10)],
        [remove("first.parquet")],
        [remove("second.parquet")],
    ]
    if column_mapping:
        commits[0][0] = {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}}
    if deletion_vectors:
        commits[0][0] = {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
                                     "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]}}
        original = commits[0][-1]["add"]
        commits = [commits[0]]
        previous = None
        for rows in ([0, 1], [2, 3], list(range(6))):
            removal = remove("first.parquet")
            if previous is not None:
                removal["remove"]["deletionVector"] = previous
            descriptor = inline_deletion_vector(rows)
            commits.append([removal, {"add": dict(original, deletionVector=descriptor)}])
            previous = descriptor
    for version, actions in enumerate(commits):
        (log / f"{version:020d}.json").write_text("".join(json.dumps(action) + "\n" for action in actions))


def inline_deletion_vector(rows):
    # Delta's portable RoaringTreemap frame: magic, one high-32-bit key,
    # and one no-run array container. These fixtures use only small row IDs.
    # This deliberately bounded generator needs no bitmap library or binary file.
    assert rows == sorted(set(rows)) and 0 < len(rows) <= 4096
    assert all(0 <= row < 65536 for row in rows)
    payload = struct.pack("<IQIIIHHI", 1681511377, 1, 0, 12346, 1, 0, len(rows) - 1, 16)
    payload += struct.pack("<" + "H" * len(rows), *rows)
    assert len(payload) % 4 == 0  # Even cardinalities in these fixtures.
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
    encoded = []
    for offset in range(0, len(payload), 4):
        value = int.from_bytes(payload[offset:offset + 4], "big")
        digits = []
        for _ in range(5):
            value, digit = divmod(value, 85)
            digits.append(alphabet[digit])
        encoded.extend(reversed(digits))
    return {"storageType": "i", "pathOrInlineDv": "".join(encoded),
            "sizeInBytes": len(payload), "cardinality": len(rows)}


if __name__ == "__main__":
    generate(Path(sys.argv[1]), deletion_vectors="--deletion-vectors" in sys.argv[2:],
             column_mapping="--column-mapping" in sys.argv[2:], partitioned="--partitioned" in sys.argv[2:],
             field_types="--field-types" in sys.argv[2:])
