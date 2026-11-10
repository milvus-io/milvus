"""Deterministic D2 rows and a separate runtime field-alignment oracle."""

SCALAR_FIELDS = ["ts", "flag", "i8", "i16", "i32", "i64", "f32", "f64", "text", "optional"]


def row(number, string_pk=False, primary=None):
    primary = number if primary is None else primary
    return {
        "id": f"id_{primary:05d}" if string_pk else primary,
        "ts": number,
        "flag": number % 2 == 0,
        "i8": number % 127,
        "i16": number % 32767,
        "i32": number * 3,
        "i64": number * 5,
        "f32": number * 0.25,
        "f64": number * 0.125,
        "text": f"row_{number}",
        "optional": None if number % 3 == 0 else number,
        "payload": {"boost": number * 7},
        "dynamic_boost": number * 11,
        "array": [number, number + 1],
        "vector": [float(number % 17), float(number % 19)],
    }


SCHEMA_MODULE = """import os
import pyarrow as pa
class UDF:
    def __init__(self, context):
        self.context = context
    def transform_query(self, params, columns):
        if params.get("mode") != "schema":
            return [pa.array([v.as_py() * 2 for v in columns[-1]], type=pa.float32())]
        types = [pa.int64(), pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                 pa.float32(), pa.float64(), pa.string(), pa.int64()]
        if [c.type for c in columns[:10]] != types:
            raise AssertionError("scalar Arrow types changed")
        for values in zip(*(c.to_pylist() for c in columns)):
            n = values[0]
            expected = (n, n % 2 == 0, n % 127, n % 32767, n * 3, n * 5,
                        n / 4, n / 8, "row_" + str(n), None if n % 3 == 0 else n)
            if values[:10] != expected:
                raise AssertionError(("scalar alignment", values[:10], expected))
            if len(values) == 11 and values[10] != (99 if n >= 20000 else None):
                raise AssertionError("added field old/new row mismatch")
        self.context.logger.warning("PYUDF_R2_SCHEMA resource=%s pid=%s rows=%s columns=%s",
            self.context.resource_name, os.getpid(), len(columns[0]), len(columns))
        return [pa.array([v.as_py() * 2 for v in columns[0]], type=pa.float32())]
def create_udf(context):
    return UDF(context)
"""
