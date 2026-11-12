"""Exercise the proposed inline BLOB encoding with the Milvus wire schema."""

import unittest

from schema_pb2 import FieldData


def encode_inline_rows(rows: list[bytes | None], max_bytes: int) -> bytes:
    """Encode a nullable batch. The field type is unset in this experiment."""
    if max_bytes <= 0:
        raise ValueError("The byte limit must be positive")

    field = FieldData()
    # Select the bytes variant even when every row is NULL.
    field.scalars.bytes_data.SetInParent()
    for index, value in enumerate(rows):
        if value is not None:
            if len(value) > max_bytes:
                raise ValueError(f"Row {index} exceeds the {max_bytes}-byte limit")
            field.scalars.bytes_data.data.append(value)
        field.valid_data.append(value is not None)
    field.scalars.valid_data.extend(field.valid_data)
    return field.SerializeToString()


class InlineBlobWireTest(unittest.TestCase):
    def test_mixed_rows(self):
        rows = [None, b"", bytes(range(256)), None]
        field = FieldData.FromString(encode_inline_rows(rows, max_bytes=256))
        self.assertEqual(field.scalars.WhichOneof("data"), "bytes_data")
        self.assertEqual(list(field.valid_data), [False, True, True, False])
        self.assertEqual(list(field.scalars.valid_data), list(field.valid_data))
        values = iter(field.scalars.bytes_data.data)
        self.assertEqual([next(values) if valid else None for valid in field.valid_data], rows)
        self.assertEqual(list(values), [])

    def test_all_null_and_empty_batches(self):
        for rows in ([], [None, None]):
            with self.subTest(rows=rows):
                field = FieldData.FromString(encode_inline_rows(rows, max_bytes=1))
                self.assertEqual(field.scalars.WhichOneof("data"), "bytes_data")
                self.assertEqual(list(field.scalars.bytes_data.data), [])
                self.assertEqual(list(field.valid_data), [False] * len(rows))
                self.assertEqual(list(field.scalars.valid_data), list(field.valid_data))

    def test_limit_applies_to_each_value(self):
        field = FieldData.FromString(encode_inline_rows([b"ab", b"cd"], max_bytes=2))
        self.assertEqual(list(field.scalars.bytes_data.data), [b"ab", b"cd"])
        with self.assertRaisesRegex(ValueError, "Row 1 exceeds"):
            encode_inline_rows([None, b"abc"], max_bytes=2)

    def test_limit_must_be_positive(self):
        for limit in (0, -1):
            with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, "must be positive"):
                encode_inline_rows([], max_bytes=limit)


if __name__ == "__main__":
    unittest.main()
