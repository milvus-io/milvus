import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType

prefix = "no_vector"
default_nb = 2000
pk_field = "id"
int_field = "age"
str_field = "name"
no_vector_search_msg = "has no vector field, search is not supported"
no_vector_add_msg = "has no vector field, adding a vector field is not supported"


class TestMilvusClientCollectionWithoutVector(TestMilvusClientV2Base):
    """
    A collection created without a vector field. Everything except search is
    expected to work: insert, upsert, delete, query, get, count, scalar index,
    load without any index, partition and clustering keys, adding scalar
    fields. Search returns one error, and a vector field cannot be added.
    """

    def _create_without_vector(self, client, collection_name, partition_key=False, clustering_key=False):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(int_field, DataType.INT32, is_clustering_key=clustering_key)
        schema.add_field(str_field, DataType.VARCHAR, max_length=64, is_partition_key=partition_key)
        self.create_collection(client, collection_name, schema=schema)
        return schema

    @staticmethod
    def _rows(start, count):
        return [{pk_field: i, int_field: i % 100, str_field: f"name_{i % 10}"} for i in range(start, start + count)]

    def _count(self, client, collection_name):
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        return res[0]["count(*)"]

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_without_vector_dml_and_query(self):
        """
        target: create a collection with no vector field and run every non-search operation
        method: create, insert, load with no index, query, get, count, delete, upsert, search
        expected: all operations succeed, search is rejected with one error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create_without_vector(client, collection_name)
        assert collection_name in self.list_collections(client)[0]
        described = self.describe_collection(client, collection_name)[0]
        assert [f["name"] for f in described["fields"]] == [pk_field, int_field, str_field]

        self.insert(client, collection_name, self._rows(0, default_nb))
        self.flush(client, collection_name)
        # No index of any kind is needed to load.
        self.load_collection(client, collection_name)
        assert self._count(client, collection_name) == default_nb

        res = self.query(client, collection_name, filter=f"{int_field} == 7", output_fields=[pk_field, int_field])[0]
        assert len(res) == default_nb // 100
        assert all(r[int_field] == 7 for r in res)

        res = self.get(client, collection_name, ids=[1, 2, 3], output_fields=[str_field])[0]
        assert sorted(r[pk_field] for r in res) == [1, 2, 3]

        self.delete(client, collection_name, filter=f"{int_field} < 10")
        assert self._count(client, collection_name) == default_nb - default_nb // 10

        self.upsert(client, collection_name, self._rows(0, 5))
        res = self.get(client, collection_name, ids=[0, 1, 2, 3, 4])[0]
        assert len(res) == 5

        error = {ct.err_code: 1100, ct.err_msg: no_vector_search_msg}
        self.search(client, collection_name, [[0.0, 0.0]], limit=1, check_task=CheckTasks.err_res, check_items=error)

        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_without_vector_scalar_index_and_keys(self):
        """
        target: scalar index, partition key and clustering key on a collection with no vector field
        method: create with partition and clustering keys, insert, build INVERTED index, load, query
        expected: index is listed, load succeeds, queries return the expected rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create_without_vector(client, collection_name, partition_key=True, clustering_key=True)
        self.insert(client, collection_name, self._rows(0, default_nb))
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(int_field, index_type="INVERTED")
        self.create_index(client, collection_name, index_params)
        assert self.list_indexes(client, collection_name)[0] == [int_field]

        self.load_collection(client, collection_name)
        assert self._count(client, collection_name) == default_nb
        res = self.query(client, collection_name, filter=f"{str_field} == 'name_3' and {int_field} > 50")[0]
        assert len(res) == default_nb // 10 // 2
        assert all(r[str_field] == "name_3" and r[int_field] > 50 for r in res)

        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_collection_without_vector_add_field(self):
        """
        target: schema evolution on a collection with no vector field
        method: add a scalar field, then try to add a vector field
        expected: the scalar field is added, the vector field is rejected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create_without_vector(client, collection_name)
        self.add_collection_field(client, collection_name, field_name="score", data_type=DataType.INT64, nullable=True)
        described = self.describe_collection(client, collection_name)[0]
        assert "score" in [f["name"] for f in described["fields"]]

        error = {ct.err_code: 1100, ct.err_msg: no_vector_add_msg}
        self.add_collection_field(
            client,
            collection_name,
            field_name="vec",
            data_type=DataType.FLOAT_VECTOR,
            dim=8,
            nullable=True,
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        described = self.describe_collection(client, collection_name)[0]
        assert "vec" not in [f["name"] for f in described["fields"]]
        self.drop_collection(client, collection_name)
