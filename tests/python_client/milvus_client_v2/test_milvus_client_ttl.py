import time
from datetime import datetime, timedelta, timezone

import pytest
from pymilvus import AnnSearchRequest, DataType, WeightedRanker
from pymilvus.orm.types import CONSISTENCY_BOUNDED, CONSISTENCY_EVENTUALLY, CONSISTENCY_SESSION, CONSISTENCY_STRONG

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from utils.util_pymilvus import *

default_nb = ct.default_nb
default_dim = ct.default_dim
default_primary_key_field_name = ct.default_primary_key_field_name
default_vector_field_name = ct.default_vector_field_name
default_int32_field_name = ct.default_int32_field_name
default_search_exp = "id >= 0"


class TestMilvusClientTTL(TestMilvusClientV2Base):
    """Test case of Time To Live"""

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("flush_enable", [True, False])
    @pytest.mark.parametrize("on_insert", [True, False])
    def test_milvus_client_ttl_default(self, flush_enable, on_insert):
        """
        Test case for verifying TTL (Time To Live) functionality in Milvus client.

        This test verifies that:
        1. Data becomes invisible after the specified TTL period
        2. Different operations (search, query, hybrid search) correctly handle expired data
        3. TTL can be altered and the changes take effect
        4. Newly inserted data is not affected by previous TTL settings

        The test performs the following steps:
        1. Create a collection with TTL enabled
        2. Insert test data
        3. Wait for TTL to expire and verifies data becomes invisible
        4. Insert new data and verify new inserted data are visible
        5. Alter TTL and verify the changes

        Parameters:
        - flush_enable: Whether to flush collection during testing
        - on_insert: Whether to use insert or upsert operation
        """
        client = self._client()
        dim = 65
        ttl = 11
        nb = 1000
        # field name constants
        pk_field = "id"
        vec_field = "embeddings"
        vec_field_2 = "embeddings_2"
        bool_field = "visible"
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(pk_field, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(vec_field_2, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(bool_field, DataType.BOOL, nullable=True)
        self.create_collection(client, collection_name, schema=schema, properties={"collection.ttl.seconds": ttl})
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["properties"]["collection.ttl.seconds"] == str(ttl)

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vec_field, index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        index_params.add_index(field_name=vec_field_2, index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)

        # load collection
        self.load_collection(client, collection_name)

        # insert data
        insert_times = 2
        for i in range(insert_times):
            start_id = i * nb
            rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=start_id)
            for row in rows:
                row[bool_field] = False
            if on_insert is True:
                self.insert(client, collection_name, rows)
            else:
                self.upsert(client, collection_name, rows)

        # search until timeout or get empty results
        start_time = time.time()
        timeout = ttl * 5
        nq = 1
        search_ttl_effective = False
        query_ttl_effective = False
        hybrid_search_ttl_effective = False
        search_vectors = cf.gen_vectors(nq, dim=dim)
        sub_search1 = AnnSearchRequest(search_vectors, vec_field, {"level": 1}, 20)
        sub_search2 = AnnSearchRequest(search_vectors, vec_field_2, {"level": 1}, 20)
        ranker = WeightedRanker(0.2, 0.8)
        # flush collection if flush_enable is True
        if flush_enable:
            t1 = time.time()
            self.flush(client, collection_name)
            log.info(f"flush completed in {time.time() - t1}s")
        while time.time() - start_time < timeout:
            if search_ttl_effective is False:
                res1 = self.search(
                    client,
                    collection_name,
                    search_vectors,
                    anns_field=vec_field,
                    search_params={"metric_type": "COSINE"},
                    limit=10,
                    consistency_level=CONSISTENCY_STRONG,
                )[0]
            if query_ttl_effective is False:
                res2 = self.query(
                    client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
                )[0]
            if hybrid_search_ttl_effective is False:
                res3 = self.hybrid_search(
                    client,
                    collection_name,
                    [sub_search1, sub_search2],
                    ranker,
                    limit=10,
                    consistency_level=CONSISTENCY_STRONG,
                )[0]
            if len(res1[0]) == 0 and search_ttl_effective is False:
                log.info(f"search ttl effects in {round(time.time() - start_time, 4)}s")
                search_ttl_effective = True
            if res2[0].get("count(*)", None) == 0 and query_ttl_effective is False:
                log.info(f"query ttl effects in {round(time.time() - start_time, 4)}s")
                res2x = self.query(
                    client,
                    collection_name,
                    filter="visible==False",
                    output_fields=["count(*)"],
                    consistency_level=CONSISTENCY_STRONG,
                )[0]
                log.debug(f"res2x: {res2x[0].get('count(*)', None)}")
                query_ttl_effective = True
            if len(res3[0]) == 0 and hybrid_search_ttl_effective is False:
                log.info(f"hybrid search ttl effects in {round(time.time() - start_time, 4)}s")
                hybrid_search_ttl_effective = True
            if search_ttl_effective is True and query_ttl_effective is True and hybrid_search_ttl_effective is True:
                break
            time.sleep(1)

        delta_tt = round(time.time() - start_time, 4)
        log.info(f"ttl effects in {delta_tt}s")
        assert ttl - 2 <= delta_tt <= ttl + 5

        # query count(*)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)", None) == 0

        # insert more data
        for i in range(insert_times):
            start_id = (insert_times + i) * nb
            rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=start_id)
            for row in rows:
                row[bool_field] = True
            if on_insert is True:
                self.insert(client, collection_name, rows)
            else:
                self.upsert(client, collection_name, rows)

        # flush collection if flush_enable is True
        if flush_enable:
            t1 = time.time()
            self.flush(client, collection_name)
            log.info(f"flush completed in {time.time() - t1}s")

        # search data again after insert more data
        consistency_levels = [CONSISTENCY_EVENTUALLY, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_STRONG]
        for consistency_level in consistency_levels:
            log.debug(f"start to search/query with {consistency_level}")
            # Poll until search returns results (search visibility may lag behind query)
            for _i in range(15):
                res = self.search(
                    client,
                    collection_name,
                    search_vectors,
                    search_params={"metric_type": "COSINE"},
                    anns_field=vec_field,
                    limit=10,
                    consistency_level=consistency_level,
                )[0]
                if len(res[0]) > 0:
                    break
                time.sleep(2)
            assert len(res[0]) > 0, f"Search with {consistency_level} returned 0 results after retries"

            if consistency_level != CONSISTENCY_STRONG:
                pass
            else:
                # query count(*)
                res = self.query(
                    client, collection_name, filter="", output_fields=["count(*)"], consistency_level=consistency_level
                )[0]
                assert res[0].get("count(*)", None) == nb * insert_times
                res = self.query(
                    client,
                    collection_name,
                    filter="visible==False",
                    output_fields=["count(*)"],
                    consistency_level=consistency_level,
                )[0]
                assert res[0].get("count(*)", None) == 0
                # query count(visible)
                res = self.query(
                    client,
                    collection_name,
                    filter="visible==True",
                    output_fields=["count(*)"],
                    consistency_level=consistency_level,
                )[0]
                assert res[0].get("count(*)", None) == nb * insert_times

            # hybrid search
            res = self.hybrid_search(
                client,
                collection_name,
                [sub_search1, sub_search2],
                ranker,
                limit=10,
                consistency_level=consistency_level,
            )[0]
            assert len(res[0]) > 0

        # alter ttl to 2000s
        self.alter_collection_properties(client, collection_name, properties={"collection.ttl.seconds": 2000})
        for consistency_level in consistency_levels:
            log.debug(f"start to search/query after alter ttl with {consistency_level}")
            # search data after alter ttl
            res = self.search(
                client,
                collection_name,
                search_vectors,
                search_params={"metric_type": "COSINE"},
                anns_field=vec_field,
                filter="visible==False",
                limit=10,
                consistency_level=consistency_level,
                output_fields=[bool_field],
            )[0]
            assert len(res[0]) > 0
            for hit in res[0]:
                assert not hit.get(bool_field)

            # hybrid search data after alter ttl
            sub_search1 = AnnSearchRequest(search_vectors, vec_field, {"level": 1}, 20, expr="visible==False")
            sub_search2 = AnnSearchRequest(search_vectors, vec_field_2, {"level": 1}, 20, expr="visible==False")
            res = self.hybrid_search(
                client,
                collection_name,
                [sub_search1, sub_search2],
                ranker,
                limit=10,
                consistency_level=consistency_level,
            )[0]
            assert len(res[0]) > 0

            # query count(*)
            res = self.query(
                client,
                collection_name,
                filter="visible==False",
                output_fields=["count(*)"],
                consistency_level=consistency_level,
            )[0]
            assert res[0].get("count(*)", 0) == insert_times * nb
            res = self.query(
                client, collection_name, filter="", output_fields=["count(*)"], consistency_level=consistency_level
            )[0]
            if consistency_level != CONSISTENCY_STRONG:
                assert res[0].get("count(*)", 0) >= insert_times * nb
            else:
                assert res[0].get("count(*)", 0) == insert_times * nb * 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_ttl_edge(self):
        """
        Test case for verifying edge case of TTL (Time To Live) functionality in Milvus client.

        This test verifies that:
        1. Creating a collection with an extremely large TTL value should fail
        2. The system should reject TTL values that are too large (e.g., 8,640,000,000,007,819,008 seconds)

        The test performs the following steps:
        1. Attempt to create a collection with a very large TTL value
        2. Verify that the creation fails with an appropriate error

        Expected behavior:
        - Collection creation should fail when TTL is set to an extremely large value
        - An error should be raised indicating the TTL value is invalid
        """
        client = self._client()
        dim = 65
        # Set an extremely large TTL value that should cause an error
        ttl = 9223372036854775800
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=dim)
        # Attempt to create collection with extremely large TTL, expecting it to fail
        # Use force_teardown=False since collection creation should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"collection TTL is out of range, expect [-1, 3155760000], got {ttl}: invalid parameter",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={"collection.ttl.seconds": ttl},
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("partial_update", [False, True])
    def test_milvus_client_partial_update_with_ttl(self, partial_update):
        """
        target: test PU will extend the ttl of the collection
        method:
            1. Create a collection
            2. Insert rows
            3. Continuously query and search the collection
            4. Upsert the rows with partial update
            5. query and verify ttl deadline
        expected: Step 5 should success
        """
        # step 1: create collection
        ttl_time = 20
        margin = 2  # margin zone around TTL boundaries to avoid timing races
        client = self._client()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_int32_field_name, DataType.INT32, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_int32_field_name, index_type="AUTOINDEX")
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(
            client,
            collection_name,
            default_dim,
            schema=schema,
            properties={"collection.ttl.seconds": ttl_time},
            consistency_level="Strong",
            index_params=index_params,
        )

        # step 2: Insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        start_time = time.time()  # start timing right after insert to align with server-side TTL calculation
        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        # step 3: Continuously query and search the collection
        upsert_time = ttl_time / 2
        pu = True
        update_nb = default_nb // 2
        end_time = ttl_time * 2.5
        new_ttl_time = ttl_time
        while time.time() - start_time < end_time:
            # query
            # start_time ------- pu_time ------- ttl_time ------- new_ttl_time ------- end_time
            # before ttl_time, the count(*) should be default_nb
            # before new_ttl_time, and after ttl_time the count(*) should be update_nb
            # after new_ttl_time, the count(*) should be 0
            elapsed = time.time() - start_time
            res = self.query(client, collection_name, filter=default_search_exp, output_fields=["count(*)"])
            # Skip assertions near TTL boundaries to avoid timing races
            if elapsed < ttl_time - margin:
                assert res[0][0].get("count(*)") == default_nb
            elif elapsed > ttl_time + margin and elapsed < new_ttl_time - margin:
                assert res[0][0].get("count(*)") == update_nb
            elif elapsed > new_ttl_time + margin:
                assert res[0][0].get("count(*)") == 0

            # search
            # before new_ttl_time, the search result should be 10
            # after new_ttl_time, the search result should be 0
            search_vectors = cf.gen_vectors(1, dim=default_dim)
            elapsed = time.time() - start_time
            res = self.search(
                client,
                collection_name,
                search_vectors,
                anns_field=default_vector_field_name,
                search_params={"metric_type": "COSINE"},
                limit=10,
            )
            if elapsed < new_ttl_time - margin:
                assert len(res[0][0]) == 10
            elif elapsed > new_ttl_time + margin:
                assert len(res[0][0]) == 0

            time.sleep(1)
            # upsert
            if pu and time.time() - start_time >= upsert_time:
                if partial_update:
                    new_rows = cf.gen_row_data_by_schema(
                        nb=update_nb,
                        schema=schema,
                        desired_field_names=[default_primary_key_field_name, default_vector_field_name],
                    )
                else:
                    new_rows = cf.gen_row_data_by_schema(nb=update_nb, schema=schema)

                self.upsert(client, collection_name, new_rows, partial_update=partial_update)
                pu_time = time.time() - start_time
                new_ttl_time = pu_time + ttl_time
                pu = False
                time.sleep(1)

        self.drop_collection(client, collection_name)


class TestMilvusClientEntityTTLValid(TestMilvusClientV2Base):
    def _create_ttl_collection(
        self, client, collection_name, extra_fields=None, properties=None, ttl_nullable=True, **kwargs
    ):
        """Create a collection with standard TTL schema (pk + ttl + vector + index)."""
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=ttl_nullable)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        for field in extra_fields or []:
            schema.add_field(**field)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)
        if properties is None:
            properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties=properties,
            consistency_level="Strong",
            index_params=index_params,
            **kwargs,
        )

    def _wait_until_count(self, client, collection_name, expected_count, timeout=30, interval=2):
        """Poll until query count(*) equals expected_count or timeout is reached."""
        for _ in range(timeout // interval):
            res = self.query(
                client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
            )[0]
            if res[0].get("count(*)") == expected_count:
                return
            time.sleep(interval)
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == expected_count, (
            f"Expected count {expected_count}, got {res[0].get('count(*)')} after {timeout}s"
        )

    def _wait_until_search_count(
        self,
        client,
        collection_name,
        search_vectors,
        expected_count,
        anns_field=default_vector_field_name,
        timeout=30,
        interval=2,
        **search_kwargs,
    ):
        """Poll until search result count equals expected_count or timeout is reached.

        Search and query take different code paths and TTL filtering can
        propagate at different speeds, so search assertions need their own
        retry loop — mirroring ``_wait_until_count`` for query.
        """
        search_kwargs.setdefault("search_params", {})
        search_kwargs.setdefault("limit", 10)
        search_kwargs.setdefault("consistency_level", CONSISTENCY_STRONG)
        for _ in range(timeout // interval):
            res = self.search(client, collection_name, search_vectors, anns_field=anns_field, **search_kwargs)[0]
            if len(res[0]) == expected_count:
                return res
            time.sleep(interval)
        res = self.search(client, collection_name, search_vectors, anns_field=anns_field, **search_kwargs)[0]
        assert len(res[0]) == expected_count, (
            f"Expected search count {expected_count}, got {len(res[0])} after {timeout}s"
        )
        return res

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_collection_with_entity_ttl_field(self):
        """
        target: test creating collection with entity ttl_field
        method:
            1. Create a collection with ttl_field specified in properties
            2. Verify ttl_field is set correctly in collection properties
        expected: Collection created successfully with ttl_field configured
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with Timestamptz field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Verify ttl_field is set
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["properties"].get("ttl_field") == "ttl"
        assert collection_info["properties"].get("timezone") == "UTC"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_alter_add_entity_ttl_field(self):
        """
        target: test adding ttl_field to existing collection via alter
        method:
            1. Create a collection without ttl_field
            2. Use alter_collection_properties to add ttl_field
            3. Verify ttl_field is set correctly
        expected: ttl_field added successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with Timestamptz field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection without ttl_field
        properties = {"timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Alter to add ttl_field
        self.alter_collection_properties(client, collection_name, properties={"ttl_field": "ttl"})

        # Verify ttl_field is set
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["properties"].get("ttl_field") == "ttl"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_change_entity_ttl_field(self):
        """
        target: test changing ttl_field to a new field
        method:
            1. Create collection with ttl_field
            2. Use alter_collection_properties to change ttl_field to a new field
            3. Verify the new field is set as ttl_field
        expected: the new field is set as ttl_field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field("new_ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Alter to change ttl_field to new_ttl
        self.alter_collection_properties(client, collection_name, properties={"ttl_field": "new_ttl"})

        # Verify the new field is set as ttl_field
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["properties"].get("ttl_field") == "new_ttl"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_field_then_set_ttl_field(self):
        """
        target: test evolving schema by adding TIMESTAMPTZ field then setting ttl_field
        method:
            1. Create collection without TIMESTAMPTZ field
            2. Use add_field to add a TIMESTAMPTZ field
            3. Use alter_collection_properties to set ttl_field
            4. Insert data with TTL values and verify TTL behavior works
        expected: Schema evolution workflow (add_field -> set ttl_field) works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        ttl_seconds = 8

        # Create schema without TIMESTAMPTZ field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)

        # Create collection without ttl_field
        self.create_collection(
            client, collection_name, schema=schema, consistency_level="Strong", index_params=index_params
        )

        # Add TIMESTAMPTZ field via schema evolution
        self.add_collection_field(
            client, collection_name, field_name="ttl", data_type=DataType.TIMESTAMPTZ, nullable=True
        )

        # Reload collection after schema evolution
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        # Set ttl_field property
        self.alter_collection_properties(client, collection_name, properties={"ttl_field": "ttl", "timezone": "UTC"})

        # Verify ttl_field is set
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["properties"].get("ttl_field") == "ttl"

        # Insert data with future TTL
        ttl_str = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Wait for TTL to expire (poll until count reaches 0)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_database_timezone_for_entity_ttl(self):
        """
        target: test altering database timezone does not affect entity TTL expiration
        method:
            1. Create database with UTC timezone
            2. Create collection with ttl_field
            3. Insert data with timestamptz value
            4. Alter database timezone to a different timezone (e.g., Asia/Shanghai)
            5. Verify TTL expiration is not affected by timezone change
        expected: Changing database timezone does not affect expiration of existing data
                  with absolute timestamps
        """
        client = self._client()
        db_name = cf.gen_unique_str("test_db_ttl")
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100

        # Create database with UTC timezone
        self.create_database(client, db_name, properties={"timezone": "UTC"})
        self.using_database(client, db_name)

        self._create_ttl_collection(client, collection_name, properties={"ttl_field": "ttl"})

        # Insert data with future timestamp (10 seconds from now in UTC)
        ttl_timestamp = datetime.now(timezone.utc) + timedelta(seconds=10)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Alter database timezone to Asia/Shanghai (UTC+8)
        self.alter_database_properties(client, db_name, properties={"timezone": "Asia/Shanghai"})

        # Verify database timezone is changed
        db_info = self.describe_database(client, db_name)[0]
        assert db_info.get("timezone") == "Asia/Shanghai"

        # Query again to verify data is still visible (timezone change doesn't affect existing timestamps)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Wait for TTL to expire (poll until count reaches 0)
        time.sleep(10)
        self._wait_until_count(client, collection_name, expected_count=0)

        # Cleanup
        self.drop_collection(client, collection_name)
        self.using_database(client, "default")
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_with_future_entity_ttl(self):
        """
        target: test inserting data with future ttl timestamp
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() + 8 seconds
            3. Verify data is visible immediately
            4. Wait for expiration and verify data becomes invisible
        expected: Data visible before TTL, invisible after TTL expires
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert data with future ttl
        ttl_timestamp = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Wait for TTL to expire (poll until count reaches 0)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_with_expired_entity_ttl(self):
        """
        target: test inserting data with past ttl timestamp
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() - 60 seconds (already expired)
            3. Query immediately to verify data is invisible
        expected: Data is immediately invisible after insert
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100

        self._create_ttl_collection(client, collection_name)

        # Insert data with past ttl (already expired)
        ttl_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is invisible (TTL filtering may take a moment to propagate)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_with_null_entity_ttl(self):
        """
        target: test inserting data with NULL ttl (never expires) persists through
                flush, compaction, and release/reload
        method:
            1. Create collection with ttl_field (nullable=True)
            2. Insert NULL ttl data + short TTL data
            3. Wait for short TTL data to expire
            4. Verify NULL data survives after flush, compact, release/reload
        expected: Data with NULL ttl never expires and persists through all operations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert NULL ttl data (never expires) + short TTL data
        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb * 2, dim=default_dim)
        rows = []
        for i in range(nb * 2):
            ttl_value = None if i < nb else future_ttl
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify all data visible before expiry
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb * 2

        # Wait for short TTL data to expire
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=nb)

        # Flush and verify NULL data persists
        self.flush(client, collection_name)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Compact and verify NULL data persists
        self.compact(client, collection_name)
        self._wait_until_count(client, collection_name, expected_count=nb)

        # Release and reload, verify NULL data persists
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_mixed_entity_ttl_values(self):
        """
        target: test inserting data with mixed ttl values (future, past, NULL)
        method:
            1. Create collection with ttl_field
            2. Insert data with:
            - 1/3 with ttl = now() + 8 seconds (future)
            - 1/3 with ttl = now() - 60 seconds (past, already expired)
            - 1/3 with ttl = NULL (never expires)
            3. Verify only future and NULL data are visible initially
            4. Wait for future data to expire
            5. Verify only NULL data remains visible
        expected: Each data expires according to its ttl value
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 90  # 30 for each category
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Prepare ttl values
        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        past_ttl = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()

        # Insert mixed data
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = []
        for i in range(nb):
            if i < 30:
                ttl_value = future_ttl  # Future
            elif i < 60:
                ttl_value = past_ttl  # Past (expired)
            else:
                ttl_value = None  # NULL (never expires)
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify only future + NULL data are visible (30 + 30 = 60)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == 60

        # Wait for future data to expire (poll until only NULL data remains)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=30)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_upsert_partial_update_without_entity_ttl(self):
        """
        target: test partial update without ttl field preserves original ttl
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() + 10 seconds
            3. Perform partial update (only id and vector) without ttl field
            4. Verify data still expires at original ttl time
        expected: Partial update without ttl field preserves original ttl
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        ttl_seconds = 10

        self._create_ttl_collection(client, collection_name)

        # Insert data with future ttl
        ttl_timestamp = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Perform partial update without ttl field
        new_vectors = cf.gen_vectors(nb, dim=default_dim)
        update_rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(new_vectors[i])} for i in range(nb)
        ]
        self.upsert(client, collection_name, update_rows, partial_update=True)

        # Verify data is still visible
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Wait for original ttl to expire (poll until count reaches 0)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_upsert_extend_entity_ttl(self):
        """
        target: test upsert with new future TTL extends entity lifetime
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() + 8 seconds
            3. After 4 seconds, upsert same IDs with ttl = now() + 12 seconds
            4. Wait past original TTL
            5. Verify data is still visible (TTL was extended)
            6. Wait past new TTL and verify data expires
        expected: Upsert with new TTL extends the expiration deadline
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        original_ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert data with original TTL (8s)
        original_ttl = (datetime.now(timezone.utc) + timedelta(seconds=original_ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": original_ttl, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Wait 4 seconds, then upsert with extended TTL (12s from now)
        time.sleep(4)
        extended_ttl = (datetime.now(timezone.utc) + timedelta(seconds=12)).isoformat()
        upsert_rows = [
            {default_primary_key_field_name: i, "ttl": extended_ttl, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.upsert(client, collection_name, upsert_rows)

        # Wait past original TTL (4 more seconds) — data should still be alive
        time.sleep(6)
        self._wait_until_count(client, collection_name, expected_count=nb)

        # Wait past extended TTL (poll until count reaches 0)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_upsert_shorten_entity_ttl(self):
        """
        target: test upsert with shorter TTL makes entity expire sooner
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() + 60 seconds (long TTL)
            3. Upsert same IDs with ttl = now() + 8 seconds (short TTL)
            4. Wait for short TTL to expire
            5. Verify data expires at the shorter deadline
        expected: Upsert with shorter TTL overrides the original deadline
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        short_ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert data with long TTL (60s)
        long_ttl = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": long_ttl, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Upsert with short TTL (8s from now)
        short_ttl = (datetime.now(timezone.utc) + timedelta(seconds=short_ttl_seconds)).isoformat()
        upsert_rows = [
            {default_primary_key_field_name: i, "ttl": short_ttl, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.upsert(client, collection_name, upsert_rows)

        # Wait for short TTL to expire (poll until count reaches 0)
        time.sleep(short_ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_entity_ttl_after_release_reload(self):
        """
        target: test entity TTL still works after release and reload
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() + 10 seconds
            3. Release and reload collection
            4. Verify data is still visible
            5. Wait for TTL to expire
            6. Verify data is invisible after TTL expires
        expected: TTL filtering persists across release/reload cycles
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        ttl_seconds = 10

        self._create_ttl_collection(client, collection_name)

        # Insert data with future ttl
        ttl_timestamp = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_timestamp, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible before release
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Release and reload
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        # Verify data is still visible after reload
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Wait for TTL to expire (poll until count reaches 0)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_before_entity_ttl_expires(self):
        """
        target: test explicit delete removes entities immediately, not waiting for TTL
        method:
            1. Create collection with ttl_field
            2. Insert data with ttl = now() + 60 seconds (long TTL)
            3. Delete half of the entities explicitly
            4. Verify deleted entities are gone immediately
            5. Verify remaining entities are still visible
        expected: Explicit delete takes effect immediately regardless of TTL
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        ttl_seconds = 60

        self._create_ttl_collection(client, collection_name)

        # Insert data with long TTL
        ttl_timestamp = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_timestamp, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify all data is visible
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Delete first half of entities
        delete_ids = list(range(nb // 2))
        self.delete(client, collection_name, ids=delete_ids)

        # Verify deleted entities are gone immediately
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb // 2

        # Verify remaining entities are the second half
        res = self.query(
            client,
            collection_name,
            filter=f"id >= {nb // 2}",
            output_fields=[default_primary_key_field_name],
            consistency_level=CONSISTENCY_STRONG,
        )[0]
        assert len(res) == nb // 2
        for row in res:
            assert row[default_primary_key_field_name] >= nb // 2

        # Verify deleted entities are not returned even by ID filter
        res = self.query(
            client,
            collection_name,
            filter=f"id < {nb // 2}",
            output_fields=["count(*)"],
            consistency_level=CONSISTENCY_STRONG,
        )[0]
        assert res[0].get("count(*)") == 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_with_timezone_offset_entity_ttl(self):
        """
        target: test entity TTL with explicit timezone offset timestamps
        method:
            1. Create collection with ttl_field
            2. Insert data using timestamps with explicit timezone offset (e.g., +08:00)
            3. Insert data using second-level precision (no fractional seconds)
            4. Verify data is visible before TTL, invisible after TTL
        expected: Different timestamp formats (timezone offsets, different precisions)
                  are handled correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 30
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert data with explicit timezone offset (+08:00)
        now_utc = datetime.now(timezone.utc)
        future_utc = now_utc + timedelta(seconds=ttl_seconds)
        # Convert to +08:00 offset format
        tz_shanghai = timezone(timedelta(hours=8))
        future_shanghai = future_utc.astimezone(tz_shanghai)
        ttl_with_offset = future_shanghai.strftime("%Y-%m-%dT%H:%M:%S+08:00")

        # Insert with second-level precision (no fractional seconds)
        ttl_second_precision = future_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = []
        for i in range(nb):
            if i < 15:
                ttl_value = ttl_with_offset  # Explicit timezone offset
            else:
                ttl_value = ttl_second_precision  # Second-level precision with Z suffix
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify all data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Wait for TTL to expire (poll until count reaches 0)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_new_data_after_entity_ttl_expiry(self):
        """
        target: test inserting new data after previous data expired by entity TTL
        method:
            1. Create collection with ttl_field
            2. Insert first batch with ttl = now() + 8 seconds
            3. Wait for TTL to expire, verify data is gone
            4. Re-insert using the same PKs with new future TTL
            5. Insert additional batch with new PKs
            6. Verify all new data is visible and queryable
        expected: New inserts work normally after previous data expired.
                  Expired PKs can be reused as open slots for new data — even though
                  physical deletion (compaction) may not have completed, the expired
                  data is logically invisible, and re-inserting with the same PKs
                  succeeds via upsert semantics.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert first batch with short TTL
        ttl_str = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify first batch is visible
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Wait for TTL to expire (poll until count reaches 0)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        # Verify first batch is gone
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == 0

        # Re-insert using the same PKs (0..nb-1) — should succeed since originals are expired
        reuse_ttl_str = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        reuse_vectors = cf.gen_vectors(nb, dim=default_dim)
        reuse_rows = [
            {default_primary_key_field_name: i, "ttl": reuse_ttl_str, default_vector_field_name: list(reuse_vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, reuse_rows)
        self.flush(client, collection_name)

        # Verify reused PKs are visible
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Verify the reused PK data is queryable by ID
        res = self.query(
            client,
            collection_name,
            filter="id >= 0 and id < 10",
            output_fields=[default_primary_key_field_name, "ttl"],
            consistency_level=CONSISTENCY_STRONG,
        )[0]
        assert len(res) == 10

        # Insert additional batch with new PKs
        new_ttl_str = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        new_vectors = cf.gen_vectors(nb, dim=default_dim)
        new_rows = [
            {
                default_primary_key_field_name: nb + i,
                "ttl": new_ttl_str,
                default_vector_field_name: list(new_vectors[i]),
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, new_rows)
        self.flush(client, collection_name)

        # Verify both batches are visible (reused PKs + new PKs)
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb * 2

        # Verify search returns results from both batches
        search_vectors = cf.gen_vectors(1, dim=default_dim)
        self._wait_until_search_count(client, collection_name, search_vectors, expected_count=10)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_filter_expired_entity_data(self):
        """
        target: test query automatically filters expired data after TTL expiry
        method:
            1. Create collection with ttl_field, insert mixed TTL data
            2. Verify count before expiry (future + NULL = visible)
            3. Wait for TTL to expire
            4. Query and verify only NULL data remains
        expected: Query automatically filters expired data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 300
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        past_ttl = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = []
        for i in range(nb):
            if i < 100:
                ttl_value = future_ttl
            elif i < 200:
                ttl_value = past_ttl
            else:
                ttl_value = None
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Before expiry: future (100) + NULL (100) = 200 visible; past (100) already expired
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == 200

        # Wait for TTL to expire (poll until only NULL data remains)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=100)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_filter_expired_entity_data(self):
        """
        target: test search filters expired data when filter expression is present
        method:
            1. Create collection with ttl_field, insert future TTL + NULL TTL data
            2. Wait for TTL to expire
            3. Search with filter and verify only NULL TTL data is returned
        expected: Search results do not include expired data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 200
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = []
        for i in range(nb):
            ttl_value = future_ttl if i < 100 else None
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Wait for TTL to expire (poll until only NULL data remains)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=100)

        # Poll until search only returns non-expired entities.
        # Search TTL filtering can lag behind query, so retry.
        search_vectors = cf.gen_vectors(1, dim=default_dim)
        for _ in range(15):
            res = self.search(
                client,
                collection_name,
                search_vectors,
                anns_field=default_vector_field_name,
                search_params={},
                limit=10,
                filter="id >= 0",
                consistency_level=CONSISTENCY_STRONG,
            )[0]
            if len(res[0]) > 0 and all(hit["id"] >= 100 for hit in res[0]):
                break
            time.sleep(2)

        assert len(res[0]) > 0
        for hit in res[0]:
            assert hit["id"] >= 100

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_without_filter_expired_entity_data(self):
        """
        target: test search without filter also filters expired data
        method:
            1. Create collection with ttl_field, insert future TTL + NULL TTL data
            2. Wait for TTL to expire
            3. Search without filter and verify only NULL TTL data is returned
        expected: TTL filtering is applied regardless of whether a filter expression
                  is present. Search without filter should not return expired entities.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 200
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = []
        for i in range(nb):
            ttl_value = future_ttl if i < 100 else None
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Wait for TTL to expire (poll until only NULL data remains)
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=100)

        # Poll until search only returns non-expired (NULL TTL) entities.
        # Search TTL filtering can lag behind query, so retry.
        search_vectors = cf.gen_vectors(1, dim=default_dim)
        for _ in range(15):
            res = self.search(
                client,
                collection_name,
                search_vectors,
                anns_field=default_vector_field_name,
                search_params={},
                limit=10,
                consistency_level=CONSISTENCY_STRONG,
            )[0]
            if len(res[0]) > 0 and all(hit["id"] >= 100 for hit in res[0]):
                break
            time.sleep(2)

        assert len(res[0]) > 0
        for hit in res[0]:
            assert hit["id"] >= 100, (
                f"Search without filter returned expired entity id={hit['id']} (expected only id >= 100)"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_entity_ttl_field(self):
        """
        target: test query can output ttl field values
        method:
            1. Create collection with ttl_field, insert data with NULL ttl
            2. Query with output_fields including ttl
            3. Verify ttl values are returned correctly
        expected: ttl field values are returned in query results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 10

        self._create_ttl_collection(
            client,
            collection_name,
            extra_fields=[
                {"field_name": "varchar_field", "datatype": DataType.VARCHAR, "max_length": 100, "nullable": True}
            ],
        )

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {
                default_primary_key_field_name: i,
                "ttl": None,
                default_vector_field_name: list(vectors[i]),
                "varchar_field": f"text_{i}",
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        res = self.query(
            client,
            collection_name,
            filter="id >= 0 and id < 5",
            output_fields=[default_primary_key_field_name, "ttl", "varchar_field"],
            consistency_level=CONSISTENCY_STRONG,
        )[0]

        assert len(res) == 5
        for row in res:
            assert default_primary_key_field_name in row
            assert "ttl" in row
            assert row["ttl"] is None

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_entity_ttl_compaction_reclaims_space(self):
        """
        target: test that compaction physically removes expired TTL data and reclaims space
        method:
            1. Create collection with ttl_field, insert data with short TTL
            2. Record row_count from collection stats before expiry
            3. Wait for TTL to expire, then trigger compaction
            4. Verify row_count decreases after compaction completes
        expected: Compaction physically deletes expired data and reduces row_count
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 200
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert data with short TTL
        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": future_ttl, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Record row_count before expiry
        stats_before = self.get_collection_stats(client, collection_name)[0]
        row_count_before = stats_before.get("row_count", 0)
        log.info(f"Row count before expiry: {row_count_before}")

        # Wait for TTL to expire
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        # Trigger compaction and wait for it to complete
        self.compact(client, collection_name)
        time.sleep(10)

        # Verify row_count decreases after compaction
        stats_after = self.get_collection_stats(client, collection_name)[0]
        row_count_after = stats_after.get("row_count", 0)
        log.info(f"Row count after compaction: {row_count_after}")
        assert row_count_after < row_count_before, (
            f"Expected row_count to decrease after compaction, before={row_count_before}, after={row_count_after}"
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_entity_ttl_with_partitions(self):
        """
        target: test entity TTL works independently across different partitions
        method:
            1. Create collection with ttl_field and two partitions
            2. Insert short TTL data into partition_a, long TTL data into partition_b
            3. Wait for short TTL to expire
            4. Verify partition_a data expired, partition_b data still visible
        expected: TTL expiration is per-entity and works correctly across partitions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        short_ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)
        self.create_partition(client, collection_name, "partition_a")
        self.create_partition(client, collection_name, "partition_b")

        # Insert short TTL data into partition_a
        short_ttl = (datetime.now(timezone.utc) + timedelta(seconds=short_ttl_seconds)).isoformat()
        vectors_a = cf.gen_vectors(nb, dim=default_dim)
        rows_a = [
            {default_primary_key_field_name: i, "ttl": short_ttl, default_vector_field_name: list(vectors_a[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows_a, partition_name="partition_a")

        # Insert long TTL data into partition_b
        long_ttl = (datetime.now(timezone.utc) + timedelta(seconds=300)).isoformat()
        vectors_b = cf.gen_vectors(nb, dim=default_dim)
        rows_b = [
            {default_primary_key_field_name: nb + i, "ttl": long_ttl, default_vector_field_name: list(vectors_b[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows_b, partition_name="partition_b")
        self.flush(client, collection_name)

        # Verify both partitions have data
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], partition_names=["partition_a"]
        )[0]
        assert res[0].get("count(*)") == nb
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], partition_names=["partition_b"]
        )[0]
        assert res[0].get("count(*)") == nb

        # Wait for short TTL to expire
        time.sleep(short_ttl_seconds)
        # Poll on total count: partition_a expired (0) + partition_b alive (nb) = nb
        self._wait_until_count(client, collection_name, expected_count=nb)

        # Verify partition_a data expired (poll in case per-partition propagation lags)
        for _ in range(15):
            res = self.query(
                client,
                collection_name,
                filter="",
                output_fields=["count(*)"],
                partition_names=["partition_a"],
                consistency_level=CONSISTENCY_STRONG,
            )[0]
            if res[0].get("count(*)") == 0:
                break
            time.sleep(2)
        assert res[0].get("count(*)") == 0, f"Expected partition_a count 0, got {res[0].get('count(*)')}"

        # Verify partition_b data still visible
        res = self.query(
            client,
            collection_name,
            filter="",
            output_fields=["count(*)"],
            partition_names=["partition_b"],
            consistency_level=CONSISTENCY_STRONG,
        )[0]
        assert res[0].get("count(*)") == nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_entity_ttl_switch_ttl_field(self):
        """
        target: test switching ttl_field from one TIMESTAMPTZ field to another
        method:
            1. Create collection with two TIMESTAMPTZ fields (ttl, ttl_dynamic),
               set ttl as ttl_field
            2. Insert data where ttl = far future (won't expire), ttl_dynamic = short TTL
            3. Verify data does NOT expire based on ttl_dynamic (not the active ttl_field)
            4. Switch ttl_field to ttl_dynamic via alter_collection_properties
            5. Verify data now expires based on ttl_dynamic
        expected: Expiration behavior follows the currently active ttl_field;
                  switching ttl_field changes which field controls expiry
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        short_ttl_seconds = 8

        # Create schema with two TIMESTAMPTZ fields
        self._create_ttl_collection(
            client,
            collection_name,
            extra_fields=[{"field_name": "ttl_dynamic", "datatype": DataType.TIMESTAMPTZ, "nullable": True}],
        )

        # ttl = far future (won't expire under original ttl_field)
        # ttl_dynamic = short TTL (will expire if switched to)
        far_future = (datetime.now(timezone.utc) + timedelta(seconds=300)).isoformat()
        short_ttl = (datetime.now(timezone.utc) + timedelta(seconds=short_ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {
                default_primary_key_field_name: i,
                "ttl": far_future,
                "ttl_dynamic": short_ttl,
                default_vector_field_name: list(vectors[i]),
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify all data is visible (ttl_field=ttl, ttl is far future)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb

        # Wait past short_ttl_seconds — data should still be alive because
        # the active ttl_field is "ttl" (far future), not "ttl_dynamic"
        time.sleep(short_ttl_seconds + 3)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get("count(*)") == nb, "Data should NOT expire based on inactive ttl_dynamic field"

        # Switch ttl_field to ttl_dynamic
        self.alter_collection_properties(client, collection_name, properties={"ttl_field": "ttl_dynamic"})

        # Verify ttl_field is updated
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info["properties"].get("ttl_field") == "ttl_dynamic"

        # ttl_dynamic timestamps are already past — data should become invisible
        # after switching (property change may take a moment to propagate)
        self._wait_until_count(client, collection_name, expected_count=0)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_entity_ttl_with_query_iterator(self):
        """
        target: test query iterator filters expired TTL data
        method:
            1. Create collection with ttl_field
            2. Insert short TTL data + NULL TTL data
            3. Wait for short TTL to expire
            4. Use query_iterator to traverse all data
            5. Verify iterator only returns non-expired (NULL TTL) data
        expected: Query iterator respects TTL filtering, expired entities are not yielded
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 50
        ttl_seconds = 8

        self._create_ttl_collection(client, collection_name)

        # Insert short TTL data (id 0~49) + NULL TTL data (id 50~99)
        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        vectors = cf.gen_vectors(nb * 2, dim=default_dim)
        rows = []
        for i in range(nb * 2):
            ttl_value = future_ttl if i < nb else None
            rows.append(
                {default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])}
            )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Wait for short TTL to expire
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=nb)

        # Use query_iterator to traverse all remaining data
        iterator = self.query_iterator(
            client, collection_name, batch_size=10, output_fields=[default_primary_key_field_name, "ttl"]
        )[0]
        iterated_ids = []
        while True:
            batch = iterator.next()
            if not batch:
                break
            for row in batch:
                iterated_ids.append(row[default_primary_key_field_name])
        iterator.close()

        # Verify iterator only returned NULL TTL data (id >= 50)
        assert len(iterated_ids) == nb, f"Expected {nb} rows from iterator, got {len(iterated_ids)}"
        for pk in iterated_ids:
            assert pk >= nb, f"Iterator returned expired entity id={pk} (expected only id >= {nb})"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_overwrite_null_ttl_with_short_ttl(self):
        """
        target: test that upserting the same PK with a short TTL overwrites the original
                NULL (never-expire) TTL, and the data expires correctly across growing
                segments, sealed segments, release/reload, and compaction
        method:
            1. Create collection with ttl_field
            2. Insert data with NULL ttl (never expires) and flush to seal the segment
            3. Upsert same PKs with ttl = now() + 5 seconds (short TTL)
            4. Wait for TTL to expire, then query and search to verify data is gone
            5. Release and reload — verify expired data (including original NULL rows)
               remains invisible
            6. Trigger compaction and verify data is still invisible
        expected: Upsert overwrites the original NULL TTL; after expiry the data is
                  invisible through query, search, release/reload, and compaction
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100
        ttl_seconds = 5

        self._create_ttl_collection(client, collection_name)

        # Step 1: Insert data with NULL ttl (never expires) and flush to seal
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": None, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify all data is visible
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Step 2: Upsert same PKs with short TTL
        short_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        upsert_rows = [
            {default_primary_key_field_name: i, "ttl": short_ttl, default_vector_field_name: list(vectors[i])}
            for i in range(nb)
        ]
        self.upsert(client, collection_name, upsert_rows)

        # Verify data is still visible before expiry
        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == nb

        # Step 3: Wait for TTL to expire, verify via query and search
        time.sleep(ttl_seconds)
        self._wait_until_count(client, collection_name, expected_count=0)

        search_vectors = cf.gen_vectors(1, dim=default_dim)
        self._wait_until_search_count(client, collection_name, search_vectors, expected_count=0)

        # Step 4: Release and reload — expired data and original NULL rows must stay invisible
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        res = self.query(
            client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG
        )[0]
        assert res[0].get("count(*)") == 0, f"Expected 0 after release/reload, got {res[0].get('count(*)')}"

        self._wait_until_search_count(client, collection_name, search_vectors, expected_count=0)

        # Step 5: Compact and verify data is still invisible
        self.compact(client, collection_name)
        time.sleep(10)

        self._wait_until_count(client, collection_name, expected_count=0)
        self._wait_until_search_count(client, collection_name, search_vectors, expected_count=0)

        self.drop_collection(client, collection_name)


class TestMilvusClientEntityTTLInvalid(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    def test_entity_ttl_field_nullable_false(self):
        """
        target: test inserting NULL to non-nullable ttl field should fail
        method:
            1. Create collection with ttl_field (nullable=False)
            2. Attempt to insert data with ttl = NULL
            3. Verify insert fails
        expected: Insert fails when ttl is NULL and field is not nullable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with non-nullable ttl field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=False)  # Not nullable
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Attempt to insert NULL ttl - should fail
        vectors = cf.gen_vectors(10, dim=default_dim)
        rows = [
            {default_primary_key_field_name: i, "ttl": None, default_vector_field_name: list(vectors[i])}
            for i in range(10)
        ]

        error = {
            ct.err_code: 1100,
            ct.err_msg: "the num_rows (0) of field (ttl) is not equal to passed num_rows (10): invalid parameter[expected=10][actual=0]",
        }
        self.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_entity_ttl_field_not_timestamptz(self):
        """
        target: test creating collection with non-Timestamptz ttl_field should fail
        method:
            1. Create schema with Int64 field
            2. Attempt to create collection with ttl_field pointing to Int64 field
            3. Verify creation fails
        expected: Collection creation fails when ttl_field is not Timestamptz type
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with Int64 field (not Timestamptz)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("int_field", DataType.INT64, nullable=True)  # Not Timestamptz
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Attempt to create collection with int_field as ttl_field - should fail
        properties = {"ttl_field": "int_field", "timezone": "UTC"}

        error = {
            ct.err_code: 1100,
            ct.err_msg: "ttl field must be timestamptz, field name = int_field: invalid parameter",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties=properties,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_entity_ttl_field_nonexistent(self):
        """
        target: test creating collection with non-existent ttl_field should fail
        method:
            1. Create schema without the specified ttl field
            2. Attempt to create collection with ttl_field pointing to non-existent field
            3. Verify creation fails
        expected: Collection creation fails when ttl_field does not exist
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema without ttl field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Attempt to create collection with non-existent ttl_field - should fail
        properties = {"ttl_field": "nonexistent_field", "timezone": "UTC"}

        error = {
            ct.err_code: 1100,
            ct.err_msg: "ttl field name nonexistent_field not found in schema: invalid parameter",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties=properties,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_collection_both_entity_and_collection_ttl(self):
        """
        target: test creating collection with both ttl_field and collection.ttl.seconds should fail
        method:
            1. Create schema with Timestamptz field
            2. Attempt to create collection with both ttl_field and collection.ttl.seconds
            3. Verify creation fails with mutual exclusion error
        expected: Collection creation fails when both TTL types are specified
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Attempt to create collection with both TTL configs - should fail
        properties = {"ttl_field": "ttl", "collection.ttl.seconds": 3600, "timezone": "UTC"}

        error = {
            ct.err_code: 1100,
            ct.err_msg: "collection TTL and ttl field cannot be set at the same time: invalid parameter",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties=properties,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_alter_add_entity_ttl_with_existing_collection_ttl(self):
        """
        target: test adding ttl_field when collection.ttl.seconds already exists should fail
        method:
            1. Create collection with collection.ttl.seconds
            2. Attempt to add ttl_field via alter
            3. Verify alter fails
        expected: Cannot add ttl_field when collection.ttl.seconds is set
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection with collection.ttl.seconds
        properties = {"collection.ttl.seconds": 3600}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Attempt to add ttl_field - should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: "collection TTL is already set, cannot be set ttl field: invalid parameter",
        }
        self.alter_collection_properties(
            client, collection_name, properties={"ttl_field": "ttl"}, check_task=CheckTasks.err_res, check_items=error
        )

        self.drop_collection(client, collection_name)
