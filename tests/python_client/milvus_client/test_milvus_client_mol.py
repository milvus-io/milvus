import pytest

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from pymilvus import DataType, Function, FunctionType

prefix = "client_mol"

MOL_TEST_DATA = [
    {"id": 1, "mol": "CCO", "dummy_vec": [0.1] * 8},
    {"id": 2, "mol": "c1ccccc1", "dummy_vec": [0.2] * 8},
    {"id": 3, "mol": "c1ccccc1O", "dummy_vec": [0.3] * 8},
    {"id": 4, "mol": "CC(=O)O", "dummy_vec": [0.4] * 8},
    {"id": 5, "mol": "CC(=O)Oc1ccccc1", "dummy_vec": [0.5] * 8},
    {"id": 6, "mol": "c1ccc(O)cc1O", "dummy_vec": [0.6] * 8},
    {"id": 7, "mol": "CCCCCC", "dummy_vec": [0.7] * 8},
    {"id": 8, "mol": "C", "dummy_vec": [0.8] * 8},
    {"id": 9, "mol": "c1ccncc1", "dummy_vec": [0.9] * 8},
    {"id": 10, "mol": "c1ccc2ccccc2c1", "dummy_vec": [1.0] * 8},
]

MOL_CONTAINS_CASES = [
    (
        "substructure_benzene",
        'MOL_CONTAINS(mol, "c1ccccc1")',
        [2, 3, 5, 6, 10],
    ),
    (
        "substructure_cco",
        'MOL_CONTAINS(mol, "CCO")',
        [1, 4, 5],
    ),
    (
        "substructure_c1ccccc1o",
        'MOL_CONTAINS(mol, "c1ccccc1O")',
        [3, 5, 6],
    ),
    (
        "substructure_c_o_o",
        'MOL_CONTAINS(mol, "C(=O)O")',
        [4, 5],
    ),
    (
        "superstructure_catechol",
        'MOL_CONTAINS("c1ccc(O)cc1O", mol)',
        [2, 3, 6, 8],
    ),
    (
        "substructure_empty",
        'MOL_CONTAINS(mol, "Br")',
        [],
    ),
    (
        "substructure_with_scalar_filter",
        'MOL_CONTAINS(mol, "c1ccccc1") and id <= 5',
        [2, 3, 5],
    ),
    (
        "substructure_with_scalar_filter_2",
        'MOL_CONTAINS(mol, "c1ccccc1") and id <= 3',
        [2, 3],
    ),
]


@pytest.mark.tags(CaseLabel.L1)
class TestMilvusClientMolBasic(TestMilvusClientV2Base):
    """Test case of mol operations"""

    def test_mol_insert_query_roundtrip(self):
        """
        target: test mol insert and query roundtrip
        method: create collection with mol field, insert and query
        expected: query returns correct mol values
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema, _ = self.create_schema(client, auto_id=False, description="test mol collection")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("mol", DataType.MOL)
        schema.add_field("dummy_vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)
        self.insert(client, collection_name, MOL_TEST_DATA)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name="dummy_vec", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        results, _ = self.query(
            client,
            collection_name=collection_name,
            filter="id in [1, 2, 3]",
            output_fields=["id", "mol"],
        )
        got = {r["id"]: r["mol"] for r in results}
        expected = {1: "CCO", 2: "c1ccccc1"}
        assert got[1] == expected[1]
        assert got[2] == expected[2]
        assert got[3] in {"c1ccccc1O", "Oc1ccccc1"}

    def test_mol_contains_filter(self):
        """
        target: test mol_contains filter
        method: insert mol data and query with mol_contains filters
        expected: returned ids match expected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema, _ = self.create_schema(client, auto_id=False, description="test mol_contains")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("mol", DataType.MOL)
        schema.add_field("dummy_vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)
        self.insert(client, collection_name, MOL_TEST_DATA)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name="dummy_vec", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        for label, expr, expected_ids in MOL_CONTAINS_CASES:
            results, _ = self.query(
                client,
                collection_name=collection_name,
                filter=expr,
                output_fields=["id"],
            )
            got_ids = sorted([r["id"] for r in results])
            assert got_ids == sorted(expected_ids), f"{label} failed"

        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        results, _ = self.query(
            client,
            collection_name=collection_name,
            filter=MOL_CONTAINS_CASES[0][1],
            output_fields=["id"],
        )
        got_ids = sorted([r["id"] for r in results])
        assert got_ids == sorted(MOL_CONTAINS_CASES[0][2])

    def test_mol_fingerprint_search(self):
        """
        target: test mol fingerprint function and search
        method: create mol fingerprint function, insert data, search by mol_fp
        expected: search returns results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema, _ = self.create_schema(client, auto_id=False, description="test mol fingerprint")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("mol", DataType.MOL)
        schema.add_field("mol_fp", DataType.BINARY_VECTOR, dim=2048)

        mol_function = Function(
            name="mol_fingerprint",
            function_type=FunctionType.MOL_FINGERPRINT,
            input_field_names=["mol"],
            output_field_names=["mol_fp"],
            params={
                "fingerprint_type": "morgan",
                "fingerprint_size": "2048",
                "radius": "2",
            },
        )
        schema.add_function(mol_function)

        self.create_collection(client, collection_name, schema=schema)
        data = [{"id": r["id"], "mol": r["mol"]} for r in MOL_TEST_DATA]
        self.insert(client, collection_name, data)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="mol_fp",
            index_type="BIN_FLAT",
            metric_type="JACCARD",
        )
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        search_params = ct.default_search_binary_params
        results, _ = self.search(
            client,
            collection_name=collection_name,
            data=["CCO"],
            anns_field="mol_fp",
            limit=3,
            output_fields=["id", "mol"],
            search_params=search_params,
        )
        assert len(results) == 1
        assert len(results[0]) > 0
        log.info(f"mol_fp search results: {results[0]}")

    def test_mol_pattern_index_contains(self):
        """
        target: test mol_pattern index with mol_contains
        method: create collections with/without PATTERN index and compare results
        expected: results are identical for mol_contains
        """
        client = self._client()
        collection_with_index = cf.gen_collection_name_by_testcase_name() + "_idx"
        collection_no_index = cf.gen_collection_name_by_testcase_name() + "_noidx"

        schema, _ = self.create_schema(client, auto_id=False, description="test mol pattern index")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("mol", DataType.MOL)
        schema.add_field("dummy_vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_with_index, schema=schema)
        self.create_collection(client, collection_no_index, schema=schema)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name="dummy_vec", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="mol", index_type="PATTERN")
        self.create_index(client, collection_with_index, index_params=index_params)

        index_params_no, _ = self.prepare_index_params(client)
        index_params_no.add_index(field_name="dummy_vec", index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_no_index, index_params=index_params_no)

        self.insert(client, collection_with_index, MOL_TEST_DATA)
        self.insert(client, collection_no_index, MOL_TEST_DATA)

        self.load_collection(client, collection_with_index)
        self.load_collection(client, collection_no_index)

        indexes, _ = self.list_indexes(client, collection_with_index)
        mol_index_found = any("mol" in str(idx) for idx in indexes)
        assert mol_index_found

        for _, expr, expected_ids in MOL_CONTAINS_CASES:
            res_idx, _ = self.query(
                client,
                collection_name=collection_with_index,
                filter=expr,
                output_fields=["id"],
            )
            res_no_idx, _ = self.query(
                client,
                collection_name=collection_no_index,
                filter=expr,
                output_fields=["id"],
            )
            got_idx = sorted([r["id"] for r in res_idx])
            got_no_idx = sorted([r["id"] for r in res_no_idx])
            assert got_idx == sorted(expected_ids)
            assert got_no_idx == sorted(expected_ids)

        self.flush(client, collection_with_index)
        self.flush(client, collection_no_index)
        self.release_collection(client, collection_with_index)
        self.release_collection(client, collection_no_index)
        self.load_collection(client, collection_with_index)
        self.load_collection(client, collection_no_index)

        res_idx, _ = self.query(
            client,
            collection_name=collection_with_index,
            filter=MOL_CONTAINS_CASES[0][1],
            output_fields=["id"],
        )
        res_no_idx, _ = self.query(
            client,
            collection_name=collection_no_index,
            filter=MOL_CONTAINS_CASES[0][1],
            output_fields=["id"],
        )
        got_idx = sorted([r["id"] for r in res_idx])
        got_no_idx = sorted([r["id"] for r in res_no_idx])
        assert got_idx == sorted(MOL_CONTAINS_CASES[0][2])
        assert got_no_idx == sorted(MOL_CONTAINS_CASES[0][2])

    @pytest.mark.parametrize("invalid_n_bit", ["abc", "0", "63", "-1", "4097", "2147483648"])
    def test_mol_pattern_index_invalid_n_bit(self, invalid_n_bit):
        """
        target: test mol_pattern index rejects invalid n_bit params
        method: create PATTERN index with invalid n_bit value
        expected: create_index returns invalid parameter error instead of backend build failure
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema, _ = self.create_schema(client, auto_id=False, description="test invalid mol pattern n_bit")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("mol", DataType.MOL)
        schema.add_field("dummy_vec", DataType.FLOAT_VECTOR, dim=8)

        self.create_collection(client, collection_name, schema=schema)

        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="mol",
            index_type="PATTERN",
            params={"n_bit": invalid_n_bit},
        )

        error = {
            ct.err_code: 1100,
            ct.err_msg: f"n_bit for MOL_PATTERN index must be an integer in [64, 4096], got {invalid_n_bit}",
        }
        self.create_index(
            client,
            collection_name,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items=error,
        )
