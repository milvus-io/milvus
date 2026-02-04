import pytest
import time
from datetime import datetime, timedelta, timezone
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from utils.util_pymilvus import *
from base.client_v2_base import TestMilvusClientV2Base
from pymilvus import DataType, AnnSearchRequest, WeightedRanker
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY

default_nb = ct.default_nb
default_dim = ct.default_dim
default_primary_key_field_name = ct.default_primary_key_field_name
default_vector_field_name = ct.default_vector_field_name
default_int32_field_name = ct.default_int32_field_name
default_search_exp = "id >= 0"
ENTITY_TTL_QUERY_COLLECTION = "test_entity_ttl_query" + cf.gen_unique_str("_")

class TestMilvusClientTTL(TestMilvusClientV2Base):
    """ Test case of Time To Live """

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
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("embeddings_2", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("visible", DataType.BOOL, nullable=True)
        self.create_collection(client, collection_name, schema=schema, properties={"collection.ttl.seconds": ttl})
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info['properties']["collection.ttl.seconds"] == str(ttl)

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="embeddings", index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        index_params.add_index(field_name="embeddings_2", index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)

        # load collection
        self.load_collection(client, collection_name)

        # insert data
        insert_times = 2
        for i in range(insert_times):
            vectors = cf.gen_vectors(nb, dim=dim)
            vectors_2 = cf.gen_vectors(nb, dim=dim)
            rows = []
            start_id = i * nb
            for j in range(nb):
                row = {
                    "id": start_id + j,
                    "embeddings": list(vectors[j]),
                    "embeddings_2": list(vectors_2[j]),
                    "visible": False
                }
                rows.append(row)
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
        sub_search1 = AnnSearchRequest(search_vectors, "embeddings", {"level": 1}, 20)
        sub_search2 = AnnSearchRequest(search_vectors, "embeddings_2", {"level": 1}, 20)
        ranker = WeightedRanker(0.2, 0.8)
        # flush collection if flush_enable is True
        if flush_enable:
            t1 = time.time()
            self.flush(client, collection_name)
            log.info(f"flush completed in {time.time() - t1}s")
        while time.time() - start_time < timeout:
            if search_ttl_effective is False:
                res1 = self.search(client, collection_name, search_vectors, anns_field='embeddings',
                                   search_params={}, limit=10, consistency_level=CONSISTENCY_STRONG)[0]
            if query_ttl_effective is False:
                res2 = self.query(client, collection_name, filter='',
                                  output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
            if hybrid_search_ttl_effective is False:
                res3 = self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker,
                                          limit=10, consistency_level=CONSISTENCY_STRONG)[0]
            if len(res1[0]) == 0 and search_ttl_effective is False:
                log.info(f"search ttl effects in {round(time.time() - start_time, 4)}s")
                search_ttl_effective = True
            if res2[0].get('count(*)', None) == 0 and query_ttl_effective is False:
                log.info(f"query ttl effects in {round(time.time() - start_time, 4)}s")
                res2x = self.query(client, collection_name, filter='visible==False',
                                   output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
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
        res = self.query(client, collection_name, filter='', output_fields=["count(*)"])[0]
        assert res[0].get('count(*)', None) == 0

        # insert more data
        for i in range(insert_times):
            vectors = cf.gen_vectors(nb, dim=dim)
            vectors_2 = cf.gen_vectors(nb, dim=dim)
            rows = []
            start_id = (insert_times + i) * nb
            for j in range(nb):
                row = {
                    "id": start_id + j,
                    "embeddings": list(vectors[j]),
                    "embeddings_2": list(vectors_2[j]),
                    "visible": True
                }
                rows.append(row)
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
            # try 3 times
            for i in range(3):
                res = self.search(client, collection_name, search_vectors,
                                  search_params={}, anns_field='embeddings',
                                  limit=10, consistency_level=consistency_level)[0]
                if len(res[0]) > 0:
                    break
                else:
                    time.sleep(1)
            assert len(res[0]) > 0

            if consistency_level != CONSISTENCY_STRONG:
                pass
            else:
                # query count(*)
                res = self.query(client, collection_name, filter='',
                                 output_fields=["count(*)"], consistency_level=consistency_level)[0]
                assert res[0].get('count(*)', None) == nb * insert_times
                res = self.query(client, collection_name, filter='visible==False',
                                 output_fields=["count(*)"], consistency_level=consistency_level)[0]
                assert res[0].get('count(*)', None) == 0
                # query count(visible)
                res = self.query(client, collection_name, filter='visible==True',
                                 output_fields=["count(*)"], consistency_level=consistency_level)[0]
                assert res[0].get('count(*)', None) == nb * insert_times

            # hybrid search
            res = self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker,
                                     limit=10, consistency_level=consistency_level)[0]
            assert len(res[0]) > 0

        # alter ttl to 2000s
        self.alter_collection_properties(client, collection_name, properties={"collection.ttl.seconds": 2000})
        for consistency_level in consistency_levels:
            log.debug(f"start to search/query after alter ttl with {consistency_level}")
            # search data after alter ttl
            res = self.search(client, collection_name, search_vectors,
                              search_params={}, anns_field='embeddings',
                              filter='visible==False', limit=10, consistency_level=consistency_level)[0]
            assert len(res[0]) > 0

            # hybrid search data after alter ttl
            sub_search1 = AnnSearchRequest(search_vectors, "embeddings", {"level": 1}, 20, expr='visible==False')
            sub_search2 = AnnSearchRequest(search_vectors, "embeddings_2", {"level": 1}, 20, expr='visible==False')
            res = self.hybrid_search(client, collection_name, [sub_search1, sub_search2], ranker,
                                     limit=10, consistency_level=consistency_level)[0]
            assert len(res[0]) > 0

            # query count(*)
            res = self.query(client, collection_name, filter='visible==False',
                             output_fields=["count(*)"], consistency_level=consistency_level)[0]
            assert res[0].get('count(*)', 0) == insert_times * nb
            res = self.query(client, collection_name, filter='',
                             output_fields=["count(*)"], consistency_level=consistency_level)[0]
            if consistency_level != CONSISTENCY_STRONG:
                assert res[0].get('count(*)', 0) >= insert_times * nb
            else:
                assert res[0].get('count(*)', 0) == insert_times * nb * 2

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
        error = {ct.err_code: 1100, ct.err_msg: f"collection TTL is out of range, expect [-1, 3155760000], got {ttl}: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema, 
                               properties={"collection.ttl.seconds": ttl},
                               check_task=CheckTasks.err_res, check_items=error)
    
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
        self.create_collection(client, collection_name, default_dim, schema=schema,
                               properties={"collection.ttl.seconds": ttl_time}, consistency_level="Strong", index_params=index_params)

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
                assert res[0][0].get('count(*)') == default_nb
            elif elapsed > ttl_time + margin and elapsed < new_ttl_time - margin:
                assert res[0][0].get('count(*)') == update_nb
            elif elapsed > new_ttl_time + margin:
                assert res[0][0].get('count(*)') == 0

            # search
            # before new_ttl_time, the search result should be 10
            # after new_ttl_time, the search result should be 0
            search_vectors = cf.gen_vectors(1, dim=default_dim)
            elapsed = time.time() - start_time
            res = self.search(client, collection_name, search_vectors, anns_field=default_vector_field_name, search_params={}, limit=10)
            if elapsed < new_ttl_time - margin:
                assert len(res[0][0]) == 10
            elif elapsed > new_ttl_time + margin:
                assert len(res[0][0]) == 0

            time.sleep(1)
            # upsert
            if pu and time.time() - start_time >= upsert_time:
                if partial_update:
                    new_rows = cf.gen_row_data_by_schema(nb=update_nb, schema=schema,
                                                        desired_field_names=[default_primary_key_field_name, default_vector_field_name])
                else:
                    new_rows = cf.gen_row_data_by_schema(nb=update_nb, schema=schema)

                self.upsert(client, collection_name, new_rows, partial_update=partial_update)
                pu_time = time.time() - start_time
                new_ttl_time = pu_time + ttl_time
                pu = False
                time.sleep(1)

        self.drop_collection(client, collection_name)


# ==================== Entity TTL Tests ==================== #
class TestMilvusClientEntityTTLValid(TestMilvusClientV2Base):
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
        assert collection_info['properties'].get("ttl_field") == "ttl"
        assert collection_info['properties'].get("timezone") == "UTC"

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
        assert collection_info['properties'].get("ttl_field") == "ttl"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip("BUG #47416")
    # BUG #47416
    def test_alter_remove_entity_ttl_field(self):
        """
        target: test removing ttl_field from collection
        method:
            1. Create a collection with ttl_field
            2. Use alter_collection_properties to remove ttl_field (set to empty string)
            3. Verify ttl_field is removed
        expected: ttl_field removed successfully, data no longer expires
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Alter to remove ttl_field
        self.alter_collection_properties(client, collection_name, properties={"ttl_field": ""})

        # Verify ttl_field is removed
        collection_info = self.describe_collection(client, collection_name)[0]
        assert collection_info['properties'].get("ttl_field", "") == ""

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
        assert collection_info['properties'].get("ttl_field") == "new_ttl"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_database_timezone_for_entity_ttl(self):
        """
        target: test altering database timezone affects entity TTL behavior
        method:
            1. Create database with UTC timezone
            2. Create collection with ttl_field
            3. Insert data with timestamptz value
            4. Alter database timezone to a different timezone (e.g., Asia/Shanghai)
            5. Verify TTL behavior is affected by timezone change
        expected: Changing database timezone affects how TTL timestamps are interpreted
        """
        client = self._client()
        db_name = cf.gen_unique_str("test_db_ttl")
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100

        # Create database with UTC timezone
        self.create_database(client, db_name, properties={"timezone": "UTC"})
        self.using_database(client, db_name)

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl"}
        self.create_collection(client, collection_name, schema=schema,
                               properties=properties, consistency_level="Strong", index_params=index_params)

        # Insert data with future timestamp (10 seconds from now in UTC)
        ttl_timestamp = datetime.now(timezone.utc) + timedelta(seconds=10)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [{default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == nb

        # Alter database timezone to Asia/Shanghai (UTC+8)
        self.alter_database_properties(client, db_name, properties={"timezone": "Asia/Shanghai"})

        # Verify database timezone is changed
        db_info = self.describe_database(client, db_name)[0]
        assert db_info.get("timezone") == "Asia/Shanghai"

        # Query again to verify data is still visible (timezone change doesn't affect existing timestamps)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == nb

        # Wait for TTL to expire
        log.info("Waiting 10 seconds for data to expire...")
        time.sleep(10)

        # Verify data expires based on original UTC timestamp
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == 0

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

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, 
                               properties=properties, consistency_level="Strong", index_params=index_params)

        # Insert data with future ttl
        ttl_timestamp = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [{default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == nb

        # Wait for TTL to expire
        log.info(f"Waiting {ttl_seconds + 3} seconds for data to expire...")
        time.sleep(ttl_seconds + 3)

        # Verify data is invisible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == 0

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

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, 
                        properties=properties, consistency_level="Strong", index_params=index_params)

        # Insert data with past ttl (already expired)
        ttl_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [{default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is invisible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_with_null_entity_ttl(self):
        """
        target: test inserting data with NULL ttl (never expires)
        method:
            1. Create collection with ttl_field (nullable=True)
            2. Insert data with ttl = NULL
            3. Verify data remains visible after significant time
        expected: Data with NULL ttl never expires
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 100

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, 
                               properties=properties, consistency_level="Strong", index_params=index_params)

        # Insert data with NULL ttl
        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [{default_primary_key_field_name: i, "ttl": None, default_vector_field_name: list(vectors[i])} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify data is visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == nb

        # Wait a bit and verify data is still visible
        time.sleep(3)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == nb

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

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, 
                               properties=properties, consistency_level="Strong", index_params=index_params)

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
            rows.append({default_primary_key_field_name: i, "ttl": ttl_value, default_vector_field_name: list(vectors[i])})

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify only future + NULL data are visible (30 + 30 = 60)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == 60

        # Wait for future data to expire
        log.info(f"Waiting {ttl_seconds + 3} seconds for future data to expire...")
        time.sleep(ttl_seconds + 3)

        # Verify only NULL data remains (30)
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert res[0].get('count(*)') == 30

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

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        # Create collection with ttl_field
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Insert data with future ttl
        ttl_timestamp = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        ttl_str = ttl_timestamp.isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = [{default_primary_key_field_name: i, "ttl": ttl_str, default_vector_field_name: list(vectors[i])} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Perform partial update without ttl field
        new_vectors = cf.gen_vectors(nb, dim=default_dim)
        update_rows = [{default_primary_key_field_name: i, default_vector_field_name: list(new_vectors[i])} for i in range(nb)]
        self.upsert(client, collection_name, update_rows, partial_update=True)

        # Verify data is still visible
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
        assert res[0].get('count(*)') == nb

        # Wait for original ttl to expire
        log.info(f"Waiting {ttl_seconds + 3} seconds for data to expire...")
        time.sleep(ttl_seconds + 3)

        # Verify data expires at original ttl time
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
        assert res[0].get('count(*)') == 0

        self.drop_collection(client, collection_name)

@pytest.mark.xdist_group("TestMilvusClientEntityTTLQuery")
class TestMilvusClientEntityTTLQuery(TestMilvusClientV2Base):
    """
    Entity TTL Query Tests - Using shared collection to reduce time cost
    """
    collection_name = ENTITY_TTL_QUERY_COLLECTION
    nb = 500
    dim = default_dim
    ttl_seconds = 10
    vectors = None
    _ttl_expired = False

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, request):
        """
        Prepare shared collection with entity ttl data for query tests
        """
        client = self._client()
        collection_name = self.collection_name
        nb = self.nb
        ttl_seconds = self.ttl_seconds

        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=100, nullable=True)

        # Create collection with ttl_field (force_teardown=False to keep shared collection across tests)
        properties = {"ttl_field": "ttl", "timezone": "UTC"}
        self.create_collection(client, collection_name, schema=schema, properties=properties, force_teardown=False)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="IVF_FLAT", metric_type="L2", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # Insert data with mixed ttl
        future_ttl = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        past_ttl = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()

        vectors = cf.gen_vectors(nb, dim=default_dim)
        rows = []
        for i in range(nb):
            if i < 200:
                ttl_value = future_ttl  # Will expire in 10 seconds
            elif i < 400:
                ttl_value = past_ttl  # Already expired
            else:
                ttl_value = None  # Never expires
            rows.append({
                default_primary_key_field_name: i,
                "ttl": ttl_value,
                default_vector_field_name: list(vectors[i]),
                "varchar_field": f"text_{i}"
            })

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Store generated vectors on class (other attributes are already class-level)
        TestMilvusClientEntityTTLQuery.vectors = vectors
        TestMilvusClientEntityTTLQuery._ttl_expired = False

        def teardown():
            try:
                if self.has_collection(self._client(), collection_name):
                    self.drop_collection(self._client(), collection_name)
            except Exception:
                pass
        request.addfinalizer(teardown)

    def _wait_for_ttl_expire(self):
        if TestMilvusClientEntityTTLQuery._ttl_expired:
            return
        log.info(f"Waiting {self.ttl_seconds + 3} seconds for data to expire...")
        time.sleep(self.ttl_seconds + 3)
        TestMilvusClientEntityTTLQuery._ttl_expired = True

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_filter_expired_entity_data(self):
        """
        target: test query automatically filters expired data
        method:
            1. Query collection with entity ttl enabled
            2. Verify only non-expired data is returned (200 future + 100 NULL = 300)
            3. Wait for ttl to expire
            4. Verify only NULL data remains (100)
        expected: Query automatically filters expired data
        """
        client = self._client()

        # Query before expiration - should get future + NULL data (300 total)
        res = self.query(client, self.collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
        assert res[0].get('count(*)') == 300

        # Wait for ttl to expire
        self._wait_for_ttl_expire()

        # Query after expiration - should only get NULL data (100)
        res = self.query(client, self.collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
        assert res[0].get('count(*)') == 100

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_count_expired_entity_data(self):
        """
        target: test count(*) aggregation excludes expired data
        method:
            1. Use count(*) to count records
            2. Verify count matches non-expired data
        expected: count(*) does not include expired data
        """
        client = self._client()

        self._wait_for_ttl_expire()

        # count(*) should match non-expired data
        res = self.query(client, self.collection_name, filter="", output_fields=["count(*)"], consistency_level=CONSISTENCY_STRONG)[0]
        count_result = res[0].get('count(*)')
        assert count_result == 100  # Only NULL data remains

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_filter_expired_entity_data(self):
        """
        target: test search filters expired data when filter expression is present
        method:
            1. Perform vector search with a filter expression
            2. Verify search results only contain non-expired data
        expected: Search results do not include expired data
        """
        client = self._client()

        self._wait_for_ttl_expire()

        # Search with filter - TTL filtering is applied when a filter expression is present
        search_vectors = cf.gen_vectors(1, dim=self.dim)
        res = self.search(client, self.collection_name, search_vectors, anns_field='vector',
                         search_params={}, limit=10, filter="id >= 0",
                         consistency_level=CONSISTENCY_STRONG)[0]

        # Should get results from NULL data only
        assert len(res[0]) > 0
        for hit in res[0]:
            # All results should be from id >= 400 (NULL ttl data)
            assert hit['id'] >= 400

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_entity_ttl_field(self):
        """
        target: test query can output ttl field values
        method:
            1. Query with output_fields including ttl
            2. Verify ttl values are returned correctly
        expected: ttl field values are returned in query results
        """
        client = self._client()

        # Query with ttl field in output
        res = self.query(client, self.collection_name, filter="id >= 400 and id < 405",
                        output_fields=[default_primary_key_field_name, "ttl", "varchar_field"], consistency_level=CONSISTENCY_STRONG)[0]

        assert len(res) == 5
        for row in res:
            assert default_primary_key_field_name in row
            assert "ttl" in row
            assert row["ttl"] is None  # NULL ttl data


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
        rows = [{default_primary_key_field_name: i, "ttl": None, default_vector_field_name: list(vectors[i])} for i in range(10)]

        error = {ct.err_code: 1100, ct.err_msg: "the num_rows (0) of field (ttl) is not equal to passed num_rows (10): invalid parameter[expected=10][actual=0]"}
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

        error = {ct.err_code: 1100, ct.err_msg: "ttl field must be timestamptz, field name = int_field: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema, properties=properties,
                              check_task=CheckTasks.err_res, check_items=error)

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

        error = {ct.err_code: 1100, ct.err_msg: "ttl field name nonexistent_field not found in schema: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema, properties=properties,
                              check_task=CheckTasks.err_res, check_items=error)

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
        properties = {
            "ttl_field": "ttl",
            "collection.ttl.seconds": 3600,
            "timezone": "UTC"
        }

        error = {ct.err_code: 1100, ct.err_msg: "collection TTL and ttl field cannot be set at the same time: invalid parameter"}
        self.create_collection(client, collection_name, schema=schema, properties=properties,
                              check_task=CheckTasks.err_res, check_items=error)

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
        error = {ct.err_code: 1100, ct.err_msg: "collection TTL is already set, cannot be set ttl field: invalid parameter"}
        self.alter_collection_properties(client, collection_name, properties={"ttl_field": "ttl"},
                                        check_task=CheckTasks.err_res, check_items=error)

        self.drop_collection(client, collection_name)

