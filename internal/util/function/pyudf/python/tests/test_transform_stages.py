from types import MappingProxyType

import pyarrow as pa
import pytest

from milvus_pyudf_runtime.errors import PyUDFExecutionError
from milvus_pyudf_runtime.instance import PyUDFInstance


def test_ingestion_returns_list_of_struct_for_chunking():
    class Chunker:
        def transform(self, params, columns):
            return [
                pa.array(
                    [[{"text": word, "tags": ["word"]} for word in row.as_py().split()] for row in columns[0]],
                    type=pa.list_(pa.struct([("text", pa.string()), ("tags", pa.list_(pa.string()))])),
                )
            ]

    outputs = PyUDFInstance(Chunker()).execute(MappingProxyType({}), [pa.array(["one two", ""])], "ingestion")
    assert outputs[0].to_pylist() == [[{"text": "one", "tags": ["word"]}, {"text": "two", "tags": ["word"]}], []]
    assert len(outputs[0]) == 2


@pytest.mark.parametrize("stage", ["pre_process", "post_process", "l2_rerank"])
def test_query_stage_can_return_vectors_and_nested_highlights(stage):
    class Model:
        def transform_query(self, params, columns):
            return [
                pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.list_(pa.float32(), 2)),
                pa.array([{"span": "one"}, None], type=pa.struct([("span", pa.string())])),
            ]

    outputs = PyUDFInstance(Model()).execute(MappingProxyType({}), [pa.array(["one", "two"])], stage)
    assert outputs[0].to_pylist() == [[1, 2], [3, 4]]
    assert outputs[1].to_pylist() == [{"span": "one"}, None]


def test_wrong_callable_is_rejected_before_model_runs():
    class Query:
        def transform_query(self, params, columns):
            pytest.fail("model should not run")

    with pytest.raises(PyUDFExecutionError, match="does not implement transform for stage ingestion"):
        PyUDFInstance(Query()).execute(MappingProxyType({}), [pa.array([1])], "ingestion")


def test_nested_unsupported_type_is_rejected():
    class Invalid:
        def transform(self, params, columns):
            return [pa.array(["one"]).dictionary_encode()]

    with pytest.raises(PyUDFExecutionError, match="unsupported type"):
        PyUDFInstance(Invalid()).execute(MappingProxyType({}), [pa.array([1])], "ingestion")
