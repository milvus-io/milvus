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
        Build FMINDEX with a matrix of fm_sa_sample_rate and fm_block_bytes
        values; valid ones succeed and are persisted, invalid ones are rejected
        at create_index.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=100)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        build_params = params.get("params", None)
        index_params = self.prepare_index_params(client)[0]
        index_name = cf.gen_str_by_length(10, letters_only=True)
        index_params.add_index(
            field_name=content_field_name, index_name=index_name, index_type=index_type, params=build_params
        )

        if params.get("expected", None) != success:
            error, _ = self.create_index(
                client, collection_name, index_params, check_task=CheckTasks.err_res, check_items=params.get("expected")
            )
            assert error.code == params["expected"][ct.err_code], error
            return

        nb = default_nb
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row[content_field_name] = f"The {content_keywords[i % len(content_keywords)]} number {i}"
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        self.create_index(client, collection_name, index_params)
        assert self.wait_for_index_ready(client, collection_name, index_name=index_name)

        # Persisted parameters must retain their own key/value association.
        idx_info = client.describe_index(collection_name, index_name)
        assert idx_info["index_type"] == index_type
        assert idx_info["pending_index_rows"] == 0
        assert idx_info["indexed_rows"] == nb
        if build_params:
            for key, value in build_params.items():
                assert idx_info[key] == str(value)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "field_name, datatype",
        [
            ("int_field", DataType.INT64),
            ("bool_field", DataType.BOOL),
            ("float_field", DataType.FLOAT),
            ("double_field", DataType.DOUBLE),
        ],
    )
    def test_fmindex_on_non_varchar_field_rejected(self, field_name, datatype):
        """
        FMINDEX is VARCHAR-only in this release; building it on a numeric / bool
        field (INT64/BOOL/FLOAT/DOUBLE, and JSON separately below) must be
        rejected.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name, datatype=datatype)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=field_name, index_name="fm_bad", index_type=index_type, params={})
        error, _ = self.create_index(
            client,
            collection_name,
            index_params,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "FM-index can only be created on VARCHAR field"},
        )
        assert error.code == 1100, error

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
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

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

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_on_struct_sub_field_rejected(self):
        """
        A struct-array sub-field (e.g. structA[str_val] / structA[int_val]) is an
        ARRAY field to the checker regardless of the element type; FMINDEX is
        VARCHAR-only, so building on it must be rejected.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = self.create_struct_field_schema(client)[0]
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=64)
        struct_schema.add_field("int_val", DataType.INT64)
        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=10,
        )
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        for field in ["structA[str_val]", "structA[int_val]"]:
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(field_name=field, index_name="fm_struct", index_type=index_type, params={})
            error, _ = self.create_index(
                client,
                collection_name,
                index_params,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1100, "err_msg": "FM-index can only be created on VARCHAR field"},
            )
            assert error.code == 1100, error


class TestFMIndexQuery(TestMilvusClientV2Base):
    def _assert_index_ready(self, client, collection_name, index_name, expected_rows, expected_index_type):
        assert self.wait_for_index_ready(client, collection_name, index_name=index_name), (
            f"index {index_name} on {collection_name} did not become ready"
        )
        info = self.describe_index(client, collection_name, index_name)[0]
        assert info["index_name"] == index_name, info
        assert info["field_name"] == index_name, info
        assert info["index_type"] == expected_index_type, info
        assert info["state"] == "Finished", info
        assert info["pending_index_rows"] == 0, info
        assert info["total_rows"] == expected_rows, info
        assert info["indexed_rows"] == expected_rows, info

    def _build_loaded_collection(self, client):
        """Create a collection with an FMINDEX field and an identical un-indexed
        field, insert keyword data, flush (sealed), build indexes and load."""
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=2)
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(no_index_field_name, datatype=DataType.VARCHAR, max_length=600)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=600)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        insert_times = 2
        filler = "y" * 500
        for t in range(insert_times):
            rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=t * default_nb)
            for j, row in enumerate(rows):
                text = "stadium" if j % 500 == 0 else filler
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
        total_rows = insert_times * default_nb
        self._assert_index_ready(client, collection_name, vector_field_name, total_rows, "IVF_FLAT")
        self._assert_index_ready(client, collection_name, content_field_name, total_rows, "FMINDEX")
        self.load_collection(client, collection_name)
        return collection_name, insert_times, schema

    def _assert_same(self, client, collection_name, indexed_expr, scan_expr, **kwargs):
        """The indexed-field query must return exactly the same rows as the
        brute-force scan over the un-indexed twin field."""
        res_idx = self.query(client, collection_name, filter=indexed_expr, output_fields=[pk_field_name], **kwargs)[0]
        res_scan = self.query(client, collection_name, filter=scan_expr, output_fields=[pk_field_name], **kwargs)[0]
        ids_idx = sorted(r[pk_field_name] for r in res_idx)
        ids_scan = sorted(r[pk_field_name] for r in res_scan)
        assert ids_idx == ids_scan
        return ids_idx

    def _search_ids(self, client, collection_name, filter_expr, limit, expected_content):
        """Run a complete-candidate Search so its scalar filter can be compared
        with Query on the same loaded collection and indexes."""
        result_sets = client.search(
            collection_name,
            data=[[1.0] * dim],
            anns_field=vector_field_name,
            search_params={"metric_type": "COSINE", "params": {"nprobe": 128}},
            limit=limit,
            filter=filter_expr,
            output_fields=[no_index_field_name],
        )
        assert len(result_sets) == 1
        hits = result_sets[0]
        ids = [hit[pk_field_name] for hit in hits]
        assert len(ids) == len(set(ids)), "Search returned duplicate primary keys"
        distances = [hit["distance"] for hit in hits]
        assert distances == sorted(distances, reverse=True), "COSINE Search scores must be nonincreasing"
        for hit in hits:
            entity = hit.get("entity")
            assert isinstance(entity, dict), hit
            assert set(entity) == {no_index_field_name}, hit
            assert entity[no_index_field_name] == expected_content, hit
        return sorted(ids)

    def _build_twin_collection(self, client, content_fn, max_length=64, total_nb=default_nb):
        """Create a collection with an FMINDEX field and an identical un-indexed
        twin field, filled by content_fn(i) -> str, flush (sealed), build indexes
        and load. Returns the collection name."""
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=2)
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(no_index_field_name, datatype=DataType.VARCHAR, max_length=max_length)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=max_length)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rows = cf.gen_row_data_by_schema(nb=total_nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            text = content_fn(i)
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
        self._assert_index_ready(client, collection_name, vector_field_name, total_nb, "IVF_FLAT")
        self._assert_index_ready(client, collection_name, content_field_name, total_nb, "FMINDEX")
        self.load_collection(client, collection_name)
        return collection_name

    @pytest.mark.tags(CaseLabel.L0)
    def test_fmindex_prefix_infix_suffix(self):
        """
        Exact prefix / infix / suffix LIKE on the FMINDEX field must match the
        brute-force scan on the identical un-indexed field, on sealed segments.
        """
        client = self._client()
        collection_name, insert_times, _ = self._build_loaded_collection(client)
        expected_ids = sorted(
            t * default_nb + j for t in range(insert_times) for j in range(default_nb) if j % 500 == 0
        )

        # prefix: LIKE 'sta%'
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "sta%"', f'{no_index_field_name} LIKE "sta%"'
        )
        assert ids == expected_ids
        # suffix: LIKE '%ium'
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "%ium"', f'{no_index_field_name} LIKE "%ium"'
        )
        assert ids == expected_ids
        # infix: LIKE '%adi%'
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "%adi%"', f'{no_index_field_name} LIKE "%adi%"'
        )
        assert ids == expected_ids
        # no match
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "zzz%"', f'{no_index_field_name} LIKE "zzz%"'
        )
        assert len(ids) == 0
        # exact equality is NOT accelerated by FMINDEX (it declines ==/IN and
        # falls back to the raw-data scan) but must still return correct rows
        ids = self._assert_same(
            client, collection_name, f'{content_field_name} == "stadium"', f'{no_index_field_name} == "stadium"'
        )
        assert ids == expected_ids

        # Search and Query intentionally reuse this one loaded collection. With
        # IVF nprobe covering every list and limit covering all rows, the
        # FMINDEX filter result must match the raw-scan twin exactly.
        search_ids = self._search_ids(
            client,
            collection_name,
            f'{content_field_name} LIKE "sta%"',
            limit=insert_times * default_nb,
            expected_content="stadium",
        )
        scan_ids = self._assert_same(
            client, collection_name, f'{content_field_name} LIKE "sta%"', f'{no_index_field_name} LIKE "sta%"'
        )
        assert search_ids == scan_ids

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
        filler = "y" * 500
        for j, row in enumerate(rows):
            text = "stadium" if j % 500 == 0 else filler
            row[no_index_field_name] = text
            row[content_field_name] = text
        self.insert(client, collection_name, rows)

        # Strong consistency so the un-flushed growing rows are visible.
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "sta%"',
            f'{no_index_field_name} LIKE "sta%"',
            consistency_level="Strong",
        )
        expected = sorted(
            batch * default_nb + j for batch in range(insert_times + 1) for j in range(default_nb) if j % 500 == 0
        )
        assert ids == expected

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_long_text_low_hit_differential_correctness(self):
        """
        Differential correctness case for a low-hit, long-text corpus. Each row
        is ~500 characters and the marker occurs only once per 500 rows, so the
        token population is large while the candidate count stays small. This
        makes the count-first cost guard eligible to accept the pattern. The
        public test API has no stable execution-path signal, so this test proves
        result correctness against the twin-field scan only.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(no_index_field_name, datatype=DataType.VARCHAR, max_length=600)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=600)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

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
        self._assert_index_ready(client, collection_name, vector_field_name, nb, "IVF_FLAT")
        self._assert_index_ready(client, collection_name, content_field_name, nb, "FMINDEX")
        self.load_collection(client, collection_name)

        # Low-hit infix over long text: compare the indexed field with the
        # un-indexed twin. Physical FMINDEX execution is not asserted here.
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "%{marker}%"',
            f'{no_index_field_name} LIKE "%{marker}%"',
        )
        assert len(ids) == len(marked_ids) > 0
        assert set(ids) == marked_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_general_like_recheck(self):
        """
        Selective general LIKE with an interior wildcard must match the
        un-indexed twin field after FMINDEX is built over two flushed batches.
        Fragment-only rows exercise the exact phase-2 recheck, while nullable
        and empty values verify the surrounding string semantics.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(no_index_field_name, datatype=DataType.VARCHAR, max_length=600, nullable=True)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=600, nullable=True)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        batch_nb = 1500
        filler = "y" * 500
        matching_ids = set()
        qop_only_ids = set()
        zebra_only_ids = set()
        empty_ids = set()
        for batch_id in range(2):
            start = batch_id * batch_nb
            rows = cf.gen_row_data_by_schema(nb=batch_nb, schema=schema, start=start)
            for row in rows:
                pk = row[pk_field_name]
                case = pk % 500
                if case == 0:
                    text = "QOP" + filler + "ZEBRA"
                    matching_ids.add(pk)
                elif case == 1:
                    text = "QOP" + filler
                    qop_only_ids.add(pk)
                elif case == 2:
                    text = filler + "ZEBRA"
                    zebra_only_ids.add(pk)
                elif case == 3:
                    text = ""
                    empty_ids.add(pk)
                elif case == 4:
                    text = None
                else:
                    text = filler
                row[no_index_field_name] = text
                row[content_field_name] = text
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        # Each fragment occurs 12 times in roughly 1.5 million indexed tokens:
        # 12 * sample_rate(32) = 384 is below the default 0.001 cost threshold (~1500),
        # so this interior-wildcard expression takes the FMINDEX Match path.
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field_name, metric_type="COSINE", index_type="IVF_FLAT", params={"nlist": 128}
        )
        index_params.add_index(field_name=content_field_name, index_type=index_type, params={"fm_sa_sample_rate": 32})
        self.create_index(client, collection_name, index_params)
        total_rows = 2 * batch_nb
        self._assert_index_ready(client, collection_name, vector_field_name, total_rows, "IVF_FLAT")
        self._assert_index_ready(client, collection_name, content_field_name, total_rows, "FMINDEX")
        self.load_collection(client, collection_name)

        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "QOP%ZEBRA"',
            f'{no_index_field_name} LIKE "QOP%ZEBRA"',
        )
        assert set(ids) == matching_ids
        assert set(ids).isdisjoint(qop_only_ids | zebra_only_ids)

        # Empty strings match equality-style LIKE ""; nulls do not.
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE ""',
            f'{no_index_field_name} LIKE ""',
        )
        assert set(ids) == empty_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_null_rows_not_matched(self):
        """
        Nullable VARCHAR NULL rows are treated as an empty document: no pattern,
        not even LIKE '%', may match them.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=64, nullable=True)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        total_nb = default_nb
        rows = cf.gen_row_data_by_schema(nb=total_nb, schema=schema, start=0)
        non_null = 0
        for i, row in enumerate(rows):
            if i % 8 == 7:
                row[content_field_name] = None
            else:
                row[content_field_name] = content_keywords[i % len(content_keywords)]
                non_null += 1
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field_name, metric_type="COSINE", index_type="IVF_FLAT", params={"nlist": 128}
        )
        index_params.add_index(field_name=content_field_name, index_type=index_type, params={"fm_sa_sample_rate": 32})
        self.create_index(client, collection_name, index_params)
        self._assert_index_ready(client, collection_name, vector_field_name, total_nb, "IVF_FLAT")
        self._assert_index_ready(client, collection_name, content_field_name, total_nb, "FMINDEX")
        self.load_collection(client, collection_name)

        all_rows = self.query(
            client, collection_name, filter=f'{content_field_name} LIKE "%"', output_fields=[pk_field_name]
        )[0]
        all_ids = sorted(row[pk_field_name] for row in all_rows)
        expected_all = [i for i in range(total_nb) if i % 8 != 7]
        assert len(expected_all) == non_null
        assert all_ids == expected_all, "LIKE '%' must return the exact non-NULL PK set"

        sta = self.query(
            client, collection_name, filter=f'{content_field_name} LIKE "sta%"', output_fields=[pk_field_name]
        )[0]
        sta_ids = sorted(row[pk_field_name] for row in sta)
        expected_sta = [i for i in range(total_nb) if i % 8 == 0]
        assert sta_ids == expected_sta, "LIKE 'sta%' must return the exact non-NULL stadium PK set"

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_non_ascii(self):
        """
        Byte-exact substring matching over multi-byte UTF-8 content (CJK / emoji):
        a byte-level index must not mis-align on multi-byte sequences.
        """
        client = self._client()
        values = ["中文测试数据", "测试中文", "emoji😀测试", "纯英文english", "中文😀中文"]
        filler = "y" * 500

        def content_for_id(row_id):
            value_index = row_id % 500
            return values[value_index] if value_index < len(values) else filler

        total_tokens = sum(len(content_for_id(i).encode("utf-8")) for i in range(default_nb))
        collection_name = self._build_twin_collection(client, content_for_id, max_length=600)

        cases = [
            ("%测试%", {0, 1, 2}),
            ("%😀%", {2, 4}),
            ("%中文😀%", {4}),
        ]
        for pattern, matching_value_indexes in cases:
            ids = self._assert_same(
                client,
                collection_name,
                f'{content_field_name} LIKE "{pattern}"',
                f'{no_index_field_name} LIKE "{pattern}"',
            )
            expected = [i for i in range(default_nb) if i % 500 in matching_value_indexes]
            assert len(expected) * 32 < total_tokens / 1000, "fixture must satisfy the FMINDEX cost guard"
            assert ids == expected, f"unexpected UTF-8 LIKE ground truth for {pattern}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_empty_pattern_and_general_fallback(self):
        """
        `LIKE '%'` is optimized to an empty-pattern anchored operation and uses
        FMINDEX's IsNotNull fast path. General LIKE with an interior wildcard, a
        single-char `_` wildcard, and regex `=~` fall back to the scan. Every
        form must stay exact against the twin field.
        """
        client = self._client()
        collection_name = self._build_twin_collection(client, lambda i: f"req-{i % 4}-error-{i % 10}")

        all_rows = self.query(
            client, collection_name, filter=f'{content_field_name} LIKE "%"', output_fields=[pk_field_name]
        )[0]
        expected = list(range(default_nb))
        assert sorted(row[pk_field_name] for row in all_rows) == expected

        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "req-%error%"',
            f'{no_index_field_name} LIKE "req-%error%"',
        )
        assert ids == expected
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} LIKE "req-_-error-_"',
            f'{no_index_field_name} LIKE "req-_-error-_"',
        )
        assert ids == expected
        ids = self._assert_same(
            client,
            collection_name,
            f'{content_field_name} =~ "req-.-error-."',
            f'{no_index_field_name} =~ "req-.-error-."',
        )
        assert ids == expected

    @pytest.mark.tags(CaseLabel.L1)
    def test_fmindex_escaped_wildcards(self):
        r"""
        LIKE escape handling: `\%` matches a literal '%', `\_` a literal '_', and
        `\\` a literal backslash. Escaped literals are carried in the expression's
        raw string (r"...") so the backslash reaches the LIKE layer verbatim.
        """
        client = self._client()
        values = ["100%done", "under_score", "back\\slash", "plain", "50%_mixed"]
        filler = "y" * 500

        def content_for_id(row_id):
            value_index = row_id % 500
            return values[value_index] if value_index < len(values) else filler

        total_tokens = sum(len(content_for_id(i).encode("utf-8")) for i in range(default_nb))
        collection_name = self._build_twin_collection(client, content_for_id, max_length=600)

        cases = [
            (r"%\%%", {0, 4}),
            (r"%\_%", {1, 4}),
            (r"%\\%", {2}),
        ]
        for escaped, matching_value_indexes in cases:
            ids = self._assert_same(
                client,
                collection_name,
                rf'{content_field_name} LIKE r"{escaped}"',
                rf'{no_index_field_name} LIKE r"{escaped}"',
            )
            expected = [i for i in range(default_nb) if i % 500 in matching_value_indexes]
            assert len(expected) * 32 < total_tokens / 1000, "fixture must satisfy the FMINDEX cost guard"
            assert ids == expected, f"unexpected escaped LIKE ground truth for {escaped}"
