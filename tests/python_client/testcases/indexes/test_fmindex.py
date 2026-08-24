import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from idx_fmindex import FMINDEX
from pymilvus import DataType

index_type = "FMINDEX"
success = "success"
pk_field_name = "id"
vector_field_name = "vector"
content_field_name = "content_fmindex"
no_index_field_name = "content_no_index"
dim = 32
default_nb = ct.default_nb
# keywords cycled through the data; each appears default_nb / len(keywords) times
content_keywords = ["stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"]


class TestFMIndexBuildParams(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", FMINDEX.build_params)
    def test_fmindex_build_params(self, params):
        """
        Build FMINDEX with a matrix of fm_sa_sample_rate values; valid ones
        succeed and are persisted, invalid ones are rejected at create_index.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=100)
        self.create_collection(client, collection_name, schema=schema)

        nb = default_nb
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[content_field_name] = f"The {content_keywords[i % len(content_keywords)]} number {i}"
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        build_params = params.get("params", None)
        index_params = self.prepare_index_params(client)[0]
        index_name = cf.gen_str_by_length(10, letters_only=True)
        index_params.add_index(
            field_name=content_field_name, index_name=index_name, index_type=index_type, params=build_params
        )

        if params.get("expected", None) != success:
            self.create_index(
                client, collection_name, index_params, check_task=CheckTasks.err_res, check_items=params.get("expected")
            )
            return

        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=index_name)

        # persisted params (only fm_sa_sample_rate when explicitly set)
        idx_info = client.describe_index(collection_name, index_name)
        assert idx_info["index_type"] == index_type
        if build_params:
            for key, value in build_params.items():
                assert key in idx_info.keys()
                assert str(value) in idx_info.values()

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_on_non_varchar_field_rejected(self):
        """
        FMINDEX is VARCHAR-only in this release; building it on an INT64 field
        (or any non-VARCHAR field such as JSON) must be rejected.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("int_field", datatype=DataType.INT64)
        self.create_collection(client, collection_name, schema=schema)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="int_field", index_name="fm_bad", index_type=index_type, params={})
        self.create_index(
            client,
            collection_name,
            index_params,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "FM-index can only be created on VARCHAR field"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_on_json_field_rejected(self):
        """
        JSON support is a follow-up; building FMINDEX on a JSON field must be
        rejected in this release.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("json_field", datatype=DataType.JSON)
        self.create_collection(client, collection_name, schema=schema)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="json_field",
            index_name="fm_bad_json",
            index_type=index_type,
            params={"json_cast_type": "VARCHAR", "json_path": "json_field"},
        )
        self.create_index(
            client,
            collection_name,
            index_params,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "FM-index can only be created on VARCHAR field"},
        )


class TestFMIndexQuery(TestMilvusClientV2Base):
    def _build_loaded_collection(self, client):
        """Create a collection with an FMINDEX field and an identical un-indexed
        field, insert keyword data, flush (sealed), build indexes and load."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(no_index_field_name, datatype=DataType.VARCHAR, max_length=20)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=20)
        self.create_collection(client, collection_name, schema=schema)

        insert_times = 2
        for t in range(insert_times):
            rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=t * default_nb)
            for j, row in enumerate(rows):
                kw = content_keywords[j % len(content_keywords)]
                row[no_index_field_name] = kw
                row[content_field_name] = kw
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field_name, metric_type="COSINE", index_type="IVF_FLAT", params={"nlist": 128}
        )
        index_params.add_index(field_name=content_field_name, index_type=index_type, params={"fm_sa_sample_rate": 32})
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.wait_for_index_ready(client, collection_name, index_name=content_field_name)
        self.load_collection(client, collection_name)
        return collection_name, insert_times, schema

    def _assert_same(self, client, collection_name, indexed_expr, scan_expr, **kwargs):
        """The FMINDEX-accelerated query must return exactly the same rows as the
        brute-force scan over the un-indexed twin field."""
        res_idx = self.query(client, collection_name, filter=indexed_expr, output_fields=["id"], **kwargs)[0]
        res_scan = self.query(client, collection_name, filter=scan_expr, output_fields=["id"], **kwargs)[0]
        ids_idx = sorted(r["id"] for r in res_idx)
        ids_scan = sorted(r["id"] for r in res_scan)
        assert ids_idx == ids_scan
        return ids_idx

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_prefix_infix_suffix(self):
        """
        Exact prefix / infix / suffix LIKE on the FMINDEX field must match the
        brute-force scan on the identical un-indexed field, on sealed segments.
        """
        client = self._client()
        collection_name, insert_times, _ = self._build_loaded_collection(client)
        expected = insert_times * default_nb // len(content_keywords)  # rows per keyword

        # prefix: LIKE 'sta%'
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "sta%"', f'{no_index_field_name} LIKE "sta%"'
        )
        assert len(ids) == expected
        # suffix: LIKE '%ium'
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "%ium"', f'{no_index_field_name} LIKE "%ium"'
        )
        assert len(ids) == expected
        # infix: LIKE '%adi%'
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "%adi%"', f'{no_index_field_name} LIKE "%adi%"'
        )
        assert len(ids) == expected
        # no match
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "zzz%"', f'{no_index_field_name} LIKE "zzz%"'
        )
        assert len(ids) == 0
        # exact equality is NOT accelerated by FMINDEX (it declines ==/IN and
        # falls back to the raw-data scan) but must still return correct rows
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} == "park"', f'{no_index_field_name} == "park"'
        )
        assert len(ids) == expected

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_growing_and_sealed_mixed(self):
        """
        After load, insert an extra batch that stays in a GROWING segment (not
        flushed). A LIKE query with Strong consistency must return both the
        sealed rows (served by FMINDEX) and the growing rows (brute-force scan),
        proving growing falls back correctly and results are complete.
        """
        client = self._client()
        collection_name, insert_times, schema = self._build_loaded_collection(client)

        # extra batch that stays in a GROWING segment (no flush after it)
        start = insert_times * default_nb
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=start)
        for j, row in enumerate(rows):
            kw = content_keywords[j % len(content_keywords)]
            row[no_index_field_name] = kw
            row[content_field_name] = kw
        self.insert(client, collection_name, rows)

        # Strong consistency so the un-flushed growing rows are visible.
        expected = (insert_times + 1) * default_nb // len(content_keywords)
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "sta%"',
            f'{no_index_field_name} LIKE "sta%"',
            consistency_level="Strong",
        )
        assert len(ids) == expected

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_accelerated_path_long_text_low_hit(self):
        """
        Positive accelerated-path case. The other query tests use short keyword
        values where every keyword matches ~1/8 of the rows, so the count-first
        cost guard declines and they fall back to the scan (still exact, but they
        never exercise FMINDEX's own execution path). Here the corpus is long text
        (~500 chars/row) with a rare marker in only a handful of rows: total
        tokens are large and the marker's occurrence count is tiny, so the guard
        ACCEPTS the pattern and the query is actually answered by FMINDEX. The
        result must still equal the brute-force scan on the twin field and be
        non-empty.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(no_index_field_name, datatype=DataType.VARCHAR, max_length=600)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=600)
        self.create_collection(client, collection_name, schema=schema)

        nb = default_nb
        filler = "y" * 500  # marker never occurs in the filler
        marker = "ZEBRA"
        marked_ids = set()
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            text = filler + marker if i % 500 == 0 else filler  # ~nb/500 rows hit
            if i % 500 == 0:
                marked_ids.add(row[pk_field_name])
            row[no_index_field_name] = text
            row[content_field_name] = text
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field_name, metric_type="COSINE", index_type="IVF_FLAT", params={"nlist": 128}
        )
        index_params.add_index(field_name=content_field_name, index_type=index_type, params={"fm_sa_sample_rate": 32})
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.wait_for_index_ready(client, collection_name, index_name=content_field_name)
        self.load_collection(client, collection_name)

        # low-hit infix over long text: the guard accepts -> FMINDEX path, and the
        # result must match the brute-force scan on the twin field (and be non-empty)
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "%{marker}%"',
            f'{no_index_field_name} LIKE "%{marker}%"',
        )
        assert len(ids) == len(marked_ids) > 0
        assert set(ids) == marked_ids
