import base64

import numpy as np
import pytest
from base.testbase import TestBase
from pymilvus import Collection
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

# Issue #54531: a FloatVector may be sent over REST as base64 of its
# little-endian float32 bytes, as Float16Vector and BFloat16Vector already can.
# The array spelling is unchanged, and the two mix freely across rows.
#
# Bit-exact read-back goes through pymilvus (gRPC). The REST response renders a
# FloatVector as JSON numbers, and json.loads("-0") is the integer 0, so it
# cannot show that a sign or a subnormal survived.

DIM = 8
# Values a decimal spelling of a vector can lose: 0.1 has no exact decimal
# form, a subnormal is easily flushed to zero, and -0 is often written as 0.
LOSSY = [0.1, float(np.float32(1e-45)), -0.0, 1 / 3, 1e-38, -2.5, 7.0, 0.2]


def f32_bytes(values):
    return np.asarray(values, dtype="<f4").tobytes()


def b64_f32(values):
    return base64.b64encode(f32_bytes(values)).decode("ascii")


def rand_f32(dim=DIM):
    # float32 values held as Python floats, so the array spelling is exact too
    return np.random.rand(dim).astype("<f4").astype(float).tolist()


def hits(rsp):
    return [(hit["id"], hit["distance"]) for hit in rsp["data"]]


def create_collection(test, vector_fields=("vec",)):
    name = gen_collection_name("fp32b64")
    fields = [{"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}}]
    fields += [
        {"fieldName": f, "dataType": "FloatVector", "elementTypeParams": {"dim": f"{DIM}"}} for f in vector_fields
    ]
    payload = {
        "collectionName": name,
        "schema": {"autoId": False, "enableDynamicField": False, "fields": fields},
        "indexParams": [
            {"fieldName": f, "indexName": f, "metricType": "L2", "params": {"index_type": "FLAT"}}
            for f in vector_fields
        ],
    }
    rsp = test.collection_client.collection_create(payload)
    assert rsp["code"] == 0, rsp
    test.collection_client.collection_load(collection_name=name)
    test.wait_collection_load_completed(name)
    return name


@pytest.mark.tags(CaseLabel.L0)
class TestFloatVectorBase64(TestBase):
    def _read_back(self, name, field="vec"):
        res = Collection(name).query(expr="id >= 0", output_fields=[field], consistency_level="Strong")
        return {row["id"]: f32_bytes(row[field]) for row in res}

    def test_insert_base64_round_trip_bit_exact(self):
        """
        target: a base64 FloatVector is stored bit for bit, mixed with array rows
        method: insert base64, array and base64 rows in one request, read back over gRPC
        expected: every stored vector has exactly the bytes that were sent
        """
        name = create_collection(self)
        array_row, random_row = rand_f32(), rand_f32()
        rows = [
            {"id": 0, "vec": b64_f32(LOSSY)},
            {"id": 1, "vec": array_row},
            {"id": 2, "vec": b64_f32(random_row)},
        ]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == 3
        self.collection_client.flush(name)

        stored = self._read_back(name)
        assert stored[0] == f32_bytes(LOSSY)
        assert stored[1] == f32_bytes(array_row)
        assert stored[2] == f32_bytes(random_row)

        # the REST response still renders the vector as an array
        rsp = self.vector_client.vector_query(
            {"collectionName": name, "filter": "id == 2", "outputFields": ["vec"], "limit": 1}
        )
        assert rsp["code"] == 0, rsp
        assert isinstance(rsp["data"][0]["vec"], list)
        np.testing.assert_allclose(rsp["data"][0]["vec"], random_row, rtol=1e-5, atol=1e-5)

    def test_upsert_base64_round_trip_bit_exact(self):
        """
        target: upsert takes the base64 spelling too
        method: insert an array row, upsert it with a base64 vector, read back over gRPC
        expected: the stored vector is the upserted bytes
        """
        name = create_collection(self)
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": [{"id": 0, "vec": rand_f32()}]})
        assert rsp["code"] == 0, rsp
        rsp = self.vector_client.vector_upsert({"collectionName": name, "data": [{"id": 0, "vec": b64_f32(LOSSY)}]})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount"] == 1
        self.collection_client.flush(name)

        assert self._read_back(name) == {0: f32_bytes(LOSSY)}

    def test_search_base64_matches_array(self):
        """
        target: a base64 search vector is the same query as its array spelling
        method: search with nq=2 as arrays, then as base64, then via hybrid search
        expected: identical ids and distances
        """
        name = create_collection(self, vector_fields=("vec", "vec2"))
        rows = [{"id": i, "vec": rand_f32(), "vec2": rand_f32()} for i in range(200)]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(name)

        queries = [rand_f32(), rand_f32()]

        def search(data):
            rsp = self.vector_client.vector_search(
                {"collectionName": name, "annsField": "vec", "data": data, "limit": 10, "outputFields": ["id"]}
            )
            assert rsp["code"] == 0, rsp
            return hits(rsp)

        from_array = search(queries)
        assert len(from_array) > 0
        assert search([b64_f32(q) for q in queries]) == from_array

        def hybrid(first, second):
            rsp = self.vector_client.vector_hybrid_search(
                {
                    "collectionName": name,
                    "search": [
                        {"data": [first], "annsField": "vec", "limit": 10},
                        {"data": [second], "annsField": "vec2", "limit": 10},
                    ],
                    "rerank": {"strategy": "rrf", "params": {"k": 10}},
                    "limit": 10,
                    "outputFields": ["id"],
                }
            )
            assert rsp["code"] == 0, rsp
            return hits(rsp)

        q1, q2 = queries
        assert hybrid(b64_f32(q1), b64_f32(q2)) == hybrid(q1, q2)
        assert hybrid(b64_f32(q1), q2) == hybrid(q1, q2)


@pytest.mark.tags(CaseLabel.L1)
class TestFloatVectorBase64Negative(TestBase):
    @pytest.mark.parametrize(
        "value",
        [
            base64.b64encode(f32_bytes(LOSSY)[:-1]).decode(),  # dim*4 - 1 bytes
            base64.b64encode(f32_bytes(LOSSY) + b"\x00").decode(),  # dim*4 + 1 bytes
            b64_f32(LOSSY + [1.0]),  # one float too many
            b64_f32(LOSSY).rstrip("="),  # unpadded
            "not base64!",
            "",
            "null",
            "[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]",  # the text of an array
        ],
        ids=["dim*4-1", "dim*4+1", "dim+1 floats", "unpadded", "malformed", "empty", "null", "quoted array"],
    )
    def test_insert_bad_base64_rejected(self, value):
        """
        target: a bad base64 FloatVector is a parameter error naming the field
        method: insert one row with the bad value
        expected: non-zero code, and the field name in the message
        """
        name = create_collection(self)
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": [{"id": 1, "vec": value}]})
        assert rsp["code"] == 1804, rsp
        assert "vec" in rsp["message"], rsp

    @pytest.mark.parametrize(
        "value",
        [
            base64.b64encode(f32_bytes(LOSSY)[:-1]).decode(),
            b64_f32(LOSSY + [1.0]),
            "not base64!",
        ],
        ids=["dim*4-1", "dim+1 floats", "malformed"],
    )
    def test_search_bad_base64_rejected(self, value):
        """
        target: a bad base64 search vector is a parameter error
        method: search with the bad value
        expected: non-zero code
        """
        name = create_collection(self)
        rsp = self.vector_client.vector_search(
            {"collectionName": name, "annsField": "vec", "data": [value], "limit": 3}
        )
        assert rsp["code"] != 0, rsp


@pytest.mark.tags(CaseLabel.L1)
class TestFloatVectorBase64StructArray(TestBase):
    def _create(self):
        name = gen_collection_name("fp32b64")
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{DIM}"}},
                ],
                "structFields": [
                    {
                        "fieldName": "my_struct",
                        "fields": [
                            {
                                "fieldName": "sub_int",
                                "dataType": "Array",
                                "elementDataType": "Int32",
                                "elementTypeParams": {"max_capacity": 16},
                            },
                            {
                                "fieldName": "sub_vec",
                                "dataType": "ArrayOfVector",
                                "elementDataType": "FloatVector",
                                "elementTypeParams": {"dim": DIM, "max_capacity": 16},
                            },
                        ],
                    }
                ],
            },
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        for index in (
            {"fieldName": "vec", "indexName": "vec_idx", "metricType": "COSINE"},
            {"fieldName": "my_struct[sub_vec]", "indexName": "sub_vec_idx", "metricType": "MAX_SIM_COSINE"},
        ):
            rsp = self.index_client.index_create({"collectionName": name, "indexParams": [index]})
            assert rsp["code"] == 0, rsp
        rsp = self.collection_client.collection_load(collection_name=name)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(name, timeout=60)
        return name

    def test_struct_sub_vector_base64_and_embedding_list_search(self):
        """
        target: struct sub-vectors and embedding-list search take the base64 spelling
        method: insert struct elements with mixed base64/array sub_vec, then run an
                embedding-list search as arrays and as base64
        expected: insert succeeds, sub_vec reads back as sent, and both searches agree
        """
        name = self._create()
        rows = []
        for i in range(50):
            elems = [{"sub_int": i * 100 + j, "sub_vec": rand_f32()} for j in range(3)]
            rows.append({"id": i, "vec": rand_f32(), "my_struct": elems})
        # send every other sub-vector as base64
        payload_rows = [
            {
                **row,
                "my_struct": [
                    {**e, "sub_vec": b64_f32(e["sub_vec"]) if j % 2 == 0 else e["sub_vec"]}
                    for j, e in enumerate(row["my_struct"])
                ],
            }
            for row in rows
        ]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": payload_rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows)
        self.collection_client.flush(name)

        rsp = self.vector_client.vector_query(
            {"collectionName": name, "filter": "id < 5", "outputFields": ["id", "my_struct"], "limit": 5}
        )
        assert rsp["code"] == 0, rsp
        for back in rsp["data"]:
            for eb, eo in zip(back["my_struct"], rows[back["id"]]["my_struct"]):
                np.testing.assert_allclose(eb["sub_vec"], eo["sub_vec"], rtol=1e-5, atol=1e-5)

        query_list = [rand_f32() for _ in range(3)]

        def search(data):
            rsp = self.vector_client.vector_search(
                {
                    "collectionName": name,
                    "annsField": "my_struct[sub_vec]",
                    "data": data,
                    "limit": 5,
                    "outputFields": ["id"],
                }
            )
            assert rsp["code"] == 0, rsp
            return hits(rsp)

        from_array = search([query_list])
        assert len(from_array) > 0
        assert search([[b64_f32(v) for v in query_list]]) == from_array
        assert search([[b64_f32(query_list[0])] + query_list[1:]]) == from_array

    def test_struct_sub_vector_bad_base64_rejected(self):
        """
        target: a struct sub-vector with the wrong base64 length is a parameter error
        method: insert one element whose sub_vec decodes to dim*4 - 1 bytes
        expected: non-zero code naming sub_vec
        """
        name = self._create()
        bad = base64.b64encode(f32_bytes(LOSSY)[:-1]).decode()
        row = {"id": 1, "vec": rand_f32(), "my_struct": [{"sub_int": 1, "sub_vec": bad}]}
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": [row]})
        assert rsp["code"] != 0, rsp
        assert "sub_vec" in rsp["message"], rsp
