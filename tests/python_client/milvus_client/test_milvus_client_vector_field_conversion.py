import random
import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType

default_nb = 2000
dim = 8
pk_field = "id"
int_field = "age"
vec_field = "vec"
no_vector_search_msg = "has no vector field, search is not supported"
segment_max_rows_key = "collection.segment.maxRows"


class TestMilvusClientVectorFieldConversion(TestMilvusClientV2Base):
    """
    A collection moves between having and not having a vector field.

    Adding a nullable vector field to a collection without one makes it a vector
    collection: rows written earlier read the field as null and are not returned
    by search. Dropping the last vector field leaves a collection without a
    vector field: it loads with no index and search returns one error.
    """

    def _create(self, client, collection_name, with_vector):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(int_field, DataType.INT32)
        if with_vector:
            schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        if with_vector:
            self._index(client, collection_name)

    def _index(self, client, collection_name, field_name=vec_field):
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name, index_type="HNSW", metric_type="L2", params={"M": 8, "efConstruction": 64})
        self.create_index(client, collection_name, index_params)

    @staticmethod
    def _vector(seed):
        rng = random.Random(seed)
        return [rng.random() for _ in range(dim)]

    def _rows(self, start, count, with_vector):
        rows = [{pk_field: i, int_field: i % 100} for i in range(start, start + count)]
        if with_vector:
            for row in rows:
                row[vec_field] = self._vector(row[pk_field])
        return rows

    def _count(self, client, collection_name):
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"], consistency_level="Strong")[0]
        return res[0]["count(*)"]

    def _vector_fields(self, client, collection_name):
        described = self.describe_collection(client, collection_name)[0]
        return [f["name"] for f in described["fields"] if "VECTOR" in DataType(f["type"]).name]

    def _search_ids(self, client, collection_name, limit):
        res = self.search(
            client,
            collection_name,
            [self._vector(-1)],
            anns_field=vec_field,
            limit=limit,
            output_fields=[pk_field],
            consistency_level="Strong",
        )[0]
        return sorted(hit[pk_field] for hit in res[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_add_vector_field_to_collection_without_vector(self):
        """
        target: a loaded collection without a vector field gains one
        method: load with no index, add a nullable vector field, insert rows with vectors,
                then release, index and load
        expected: the collection stays loaded and queryable through the change; rows written
                  before the field exists read it as null; load needs the vector index from
                  then on; search returns only rows that have a vector
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create(client, collection_name, with_vector=False)
        self.insert(client, collection_name, self._rows(0, default_nb, with_vector=False))
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        self.add_collection_field(
            client, collection_name, field_name=vec_field, data_type=DataType.FLOAT_VECTOR, dim=dim, nullable=True
        )
        assert self._vector_fields(client, collection_name) == [vec_field]
        assert self._count(client, collection_name) == default_nb
        res = self.query(
            client, collection_name, filter=f"{pk_field} < 3", output_fields=[vec_field], consistency_level="Strong"
        )[0]
        assert len(res) == 3 and all(r[vec_field] is None for r in res)

        self.insert(client, collection_name, self._rows(default_nb, default_nb, with_vector=True))
        assert self._count(client, collection_name) == 2 * default_nb
        # The vector field has no index loaded yet.
        self.search(
            client,
            collection_name,
            [self._vector(-1)],
            anns_field=vec_field,
            limit=1,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "is not loaded"},
        )

        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        # It is a vector collection now: load needs an index on the vector field.
        self.load_collection(
            client,
            collection_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 700, ct.err_msg: "index not found"},
        )
        self._index(client, collection_name)
        self.load_collection(client, collection_name)
        assert self._count(client, collection_name) == 2 * default_nb
        assert self._search_ids(client, collection_name, limit=3 * default_nb) == list(
            range(default_nb, 2 * default_nb)
        )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_last_vector_field(self):
        """
        target: a loaded vector collection loses its only vector field
        method: drop the vector field while loaded, keep writing scalar rows, then release and load
        expected: the collection stays loaded; the vector index is removed; search returns the
                  error of a collection without a vector field; load needs no index afterwards
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create(client, collection_name, with_vector=True)
        self.insert(client, collection_name, self._rows(0, default_nb, with_vector=True))
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        assert len(self._search_ids(client, collection_name, limit=10)) == 10

        self.drop_collection_field(client, collection_name, field_name=vec_field)
        assert self._vector_fields(client, collection_name) == []
        assert self._count(client, collection_name) == default_nb
        error = {ct.err_code: 1100, ct.err_msg: no_vector_search_msg}
        self.search(
            client,
            collection_name,
            [self._vector(-1)],
            anns_field=vec_field,
            limit=1,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.insert(client, collection_name, self._rows(default_nb, default_nb, with_vector=False))
        self.flush(client, collection_name)
        assert self._count(client, collection_name) == 2 * default_nb
        self.release_collection(client, collection_name)
        # No index of any kind is needed to load.
        self.load_collection(client, collection_name)
        assert self._count(client, collection_name) == 2 * default_nb
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_and_add_vector_field_with_same_name(self):
        """
        target: a vector field dropped and added again under the same name
        method: drop the only vector field, add a nullable vector field with the old name,
                insert rows with vectors, index and load
        expected: rows written before the drop read the new field as null, not their old vector,
                  and are not returned by search
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create(client, collection_name, with_vector=True)
        self.insert(client, collection_name, self._rows(0, default_nb, with_vector=True))
        self.flush(client, collection_name)

        self.drop_collection_field(client, collection_name, field_name=vec_field)
        self.add_collection_field(
            client, collection_name, field_name=vec_field, data_type=DataType.FLOAT_VECTOR, dim=dim, nullable=True
        )
        self.insert(client, collection_name, self._rows(default_nb, default_nb, with_vector=True))
        self.flush(client, collection_name)
        self._index(client, collection_name)
        self.load_collection(client, collection_name)

        res = self.query(
            client,
            collection_name,
            filter=f"{pk_field} in [0, {default_nb - 1}]",
            output_fields=[vec_field],
            consistency_level="Strong",
        )[0]
        assert len(res) == 2 and all(r[vec_field] is None for r in res)
        assert self._search_ids(client, collection_name, limit=3 * default_nb) == list(
            range(default_nb, 2 * default_nb)
        )
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_struct_array_vector_sub_field_conversion(self):
        """
        target: a struct array field whose vector sub-field is the only vector of the collection
        method: add such a struct field to a collection without a vector field, insert, then drop it
        expected: both changes succeed; after the drop the collection has no vector field again,
                  search is rejected and it loads with no index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create(client, collection_name, with_vector=False)
        self.insert(client, collection_name, self._rows(0, default_nb, with_vector=False))
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("tag", DataType.VARCHAR, max_length=32)
        struct_schema.add_field("emb", DataType.FLOAT_VECTOR, dim=dim)
        self.add_collection_struct_field(client, collection_name, "items", struct_schema, max_capacity=4)
        rows = [
            {pk_field: i, int_field: i % 100, "items": [{"tag": "a", "emb": self._vector(i)}]}
            for i in range(default_nb, default_nb + 100)
        ]
        self.insert(client, collection_name, rows)
        assert self._count(client, collection_name) == default_nb + 100

        self.drop_collection_field(client, collection_name, field_name="items")
        assert self._count(client, collection_name) == default_nb + 100
        self.search(
            client,
            collection_name,
            [self._vector(-1)],
            anns_field="items[emb]",
            limit=1,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: no_vector_search_msg},
        )
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        assert self._count(client, collection_name) == default_nb + 100
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_segment_row_cap_survives_vector_field_conversion(self):
        """
        target: the segment row cap does not depend on the schema
        method: 1. create a collection without a vector field with collection.segment.maxRows
                2. insert several times the cap, flush
                3. add a nullable vector field, insert rows with and without a vector, flush, compact
                4. drop the vector field, insert, flush, compact
                5. set the property to an invalid value
        expected: no segment holds more than the cap plus one insert batch after any step;
                  an invalid value is rejected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        cap, batch = 20000, 4000
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(int_field, DataType.INT32)
        self.create_collection(client, collection_name, schema=schema, properties={segment_max_rows_key: str(cap)})
        described = self.describe_collection(client, collection_name)[0]
        assert described["properties"][segment_max_rows_key] == str(cap)

        def insert(start, count, with_vector):
            for begin in range(start, start + count, batch):
                self.insert(client, collection_name, self._rows(begin, batch, with_vector=with_vector))
            self.flush(client, collection_name)

        def segment_rows(total):
            # A growing segment is sealed by the insert that crosses the cap, so it holds at
            # most the cap plus one batch; a compaction output holds at most the cap.
            deadline, rows = time.time() + 120, []
            while time.time() < deadline:
                rows = sorted(
                    (s.num_rows for s in self.list_persistent_segments(client, collection_name)[0]), reverse=True
                )
                if sum(rows) == total:
                    break
                time.sleep(2)
            assert sum(rows) == total, rows
            assert max(rows) <= cap + batch, rows
            return rows

        def compact():
            job_id = self.compact(client, collection_name)[0]
            deadline = time.time() + 180
            while time.time() < deadline:
                if "Completed" in str(self.get_compaction_state(client, job_id)[0]):
                    return
                time.sleep(2)
            raise AssertionError("compaction did not complete")

        insert(0, 3 * cap, with_vector=False)
        assert len(segment_rows(3 * cap)) >= 3

        # no vector field -> vector field: the segments are not merged, new rows follow the cap
        self.add_collection_field(
            client, collection_name, field_name=vec_field, data_type=DataType.FLOAT_VECTOR, dim=dim, nullable=True
        )
        self._index(client, collection_name)
        insert(3 * cap, cap, with_vector=False)
        insert(4 * cap, cap, with_vector=True)
        compact()
        assert len(segment_rows(5 * cap)) >= 4

        # vector field -> no vector field
        self.drop_collection_field(client, collection_name, field_name=vec_field)
        insert(5 * cap, cap, with_vector=False)
        compact()
        assert len(segment_rows(6 * cap)) >= 5
        self.load_collection(client, collection_name)
        assert self._count(client, collection_name) == 6 * cap

        self.alter_collection_properties(
            client,
            collection_name,
            {segment_max_rows_key: "-1"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: f"{segment_max_rows_key} must be a non-negative integer"},
        )
        self.drop_collection_properties(client, collection_name, [segment_max_rows_key])
        assert segment_max_rows_key not in self.describe_collection(client, collection_name)[0].get("properties", {})
        self.drop_collection(client, collection_name)
