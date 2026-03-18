import time
import pytest
from faker import Faker
from pymilvus import MilvusClient, CollectionSchema
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from utils.util_common import get_collections

fake = Faker()


class TestAllCollection:
    """ Test case of end to end"""

    @pytest.fixture(scope="function", params=get_collections(file_name="chaos_test_all_collections.json"))
    def collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    @pytest.fixture(scope="function")
    def milvus_client(self):
        """Initialize MilvusClient"""
        if cf.param_info.param_uri:
            uri = cf.param_info.param_uri
        else:
            uri = "http://" + cf.param_info.param_host + ":" + str(cf.param_info.param_port)

        if cf.param_info.param_token:
            token = cf.param_info.param_token
        else:
            token = f"{cf.param_info.param_user}:{cf.param_info.param_password}"

        client = MilvusClient(uri=uri, token=token)
        yield client
        client.close()

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("skip drop collection")

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_default(self, collection_name, milvus_client):
        # create
        name = collection_name if collection_name else cf.gen_unique_str("Checker_")
        t0 = time.time()

        # Get schema from existing collection
        collection_info = milvus_client.describe_collection(collection_name=name)
        schema = CollectionSchema.construct_from_dict(collection_info)
        tt = time.time() - t0
        assert collection_info['collection_name'] == name

        # get collection info
        dim = cf.get_dim_by_schema(schema=schema)
        int64_field_name = cf.get_int64_field_name(schema=schema)
        float_vector_field_name = cf.get_float_vec_field_name(schema=schema)
        float_vector_field_name_list = cf.get_float_vec_field_name_list(schema=schema)
        text_match_fields = cf.get_text_match_field_name(schema=schema)
        bm25_vec_field_name_list = cf.get_bm25_vec_field_name_list(schema=schema)

        # compact collection before getting num_entities
        milvus_client.flush(collection_name=name, timeout=180)
        compact_job_id = milvus_client.compact(collection_name=name, timeout=180)

        # wait for compaction completed
        max_wait_time = 720
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            compact_state = milvus_client.get_compaction_state(compact_job_id)
            if compact_state == "Completed":
                break
            time.sleep(5)

        entities = milvus_client.get_collection_stats(collection_name=name).get("row_count", 0)
        log.info(f"assert create collection: {tt}, init_entities: {entities}")

        # insert
        offset = -3000
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=collection_info, start=offset)
        t0 = time.time()
        res = milvus_client.insert(collection_name=name, data=data)
        tt = time.time() - t0
        log.info(f"assert insert: {tt}")
        assert res.get('insert_count', 0) > 0

        # flush
        t0 = time.time()
        milvus_client.flush(collection_name=name, timeout=180)
        tt = time.time() - t0
        entities = milvus_client.get_collection_stats(collection_name=name).get("row_count", 0)
        log.info(f"assert flush: {tt}, entities: {entities}")

        # show index infos
        index_names = milvus_client.list_indexes(collection_name=name)
        log.info(f"index names: {index_names}")
        fields_created_index = []
        for idx_name in index_names:
            try:
                idx_info = milvus_client.describe_index(collection_name=name, index_name=idx_name)
                if 'field_name' in idx_info:
                    fields_created_index.append(idx_info['field_name'])
            except Exception as e:
                log.debug(f"Failed to describe index {idx_name}: {e}")

        # create index if not have
        index_params = milvus_client.prepare_index_params()
        for f in float_vector_field_name_list:
            if f not in fields_created_index:
                t0 = time.time()
                index_params.add_index(
                    field_name=f,
                    index_type="HNSW",
                    metric_type="L2",
                    params={"M": 48, "efConstruction": 500}
                )
                milvus_client.create_index(
                    collection_name=name,
                    index_params=index_params
                )
                tt = time.time() - t0
                log.info(f"create index for field {f} cost: {tt} seconds")
                index_params = milvus_client.prepare_index_params()  # reset for next field

        # show index infos
        index_names = milvus_client.list_indexes(collection_name=name)
        log.info(f"index names: {index_names}")

        # load
        milvus_client.load_collection(collection_name=name)

        # search
        search_vectors = cf.gen_vectors(1, dim)
        dense_search_params = {"metric_type": "L2", "params": {"ef": 64}}
        t0 = time.time()
        res_1 = milvus_client.search(
            collection_name=name,
            data=search_vectors,
            anns_field=float_vector_field_name,
            search_params=dense_search_params,
            limit=1,
            consistency_level="Strong"
        )
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res_1) == 1

        # full text search
        if len(bm25_vec_field_name_list) > 0:
            queries = [fake.text() for _ in range(1)]
            bm25_search_params = {"metric_type": "BM25", "params": {}}
            t0 = time.time()
            res_2 = milvus_client.search(
                collection_name=name,
                data=queries,
                anns_field=bm25_vec_field_name_list[0],
                search_params=bm25_search_params,
                limit=1,
                consistency_level="Strong"
            )
            tt = time.time() - t0
            log.info(f"assert full text search: {tt}")
            assert len(res_2) == 1

        # query
        term_expr = f'{int64_field_name} in {[i for i in range(offset, 0)]}'
        t0 = time.time()
        res = milvus_client.query(
            collection_name=name,
            filter=term_expr,
            limit=5,
            consistency_level="Strong"
        )
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
        assert len(res) > 0

        # text match
        if len(text_match_fields) > 0:
            queries = [fake.text().replace("\n", " ") for _ in range(1)]
            expr = f"text_match({text_match_fields[0]}, '{queries[0]}')"
            t0 = time.time()
            res = milvus_client.query(
                collection_name=name,
                filter=expr,
                limit=5,
                consistency_level="Strong"
            )
            tt = time.time() - t0
            log.info(f"assert text match: {tt}")
            assert len(res) >= 0

        # insert data
        d = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=collection_info)
        milvus_client.insert(collection_name=name, data=d)

        # release and load
        t0 = time.time()
        milvus_client.release_collection(collection_name=name)
        milvus_client.load_collection(collection_name=name)
        tt = time.time() - t0
        log.info(f"release and load: {tt}")

        # search
        nq = 5
        topk = 5
        search_vectors = cf.gen_vectors(nq, dim)
        t0 = time.time()
        res = milvus_client.search(
            collection_name=name,
            data=search_vectors,
            anns_field=float_vector_field_name,
            search_params=dense_search_params,
            limit=topk,
            consistency_level="Strong"
        )
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res) == nq
        assert len(res[0]) <= topk

        # full text search
        if len(bm25_vec_field_name_list) > 0:
            queries = [fake.text() for _ in range(1)]
            bm25_search_params = {"metric_type": "BM25", "params": {}}
            t0 = time.time()
            res_2 = milvus_client.search(
                collection_name=name,
                data=queries,
                anns_field=bm25_vec_field_name_list[0],
                search_params=bm25_search_params,
                limit=1,
                consistency_level="Strong"
            )
            tt = time.time() - t0
            log.info(f"assert full text search: {tt}")
            assert len(res_2) == 1

        # query
        term_expr = f'{int64_field_name} > -3000'
        t0 = time.time()
        res = milvus_client.query(
            collection_name=name,
            filter=term_expr,
            limit=5,
            consistency_level="Strong"
        )
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
        assert len(res) > 0

        # text match
        if len(text_match_fields) > 0:
            queries = [fake.text().replace("\n", " ") for _ in range(1)]
            expr = f"text_match({text_match_fields[0]}, '{queries[0]}')"
            t0 = time.time()
            res = milvus_client.query(
                collection_name=name,
                filter=expr,
                limit=5,
                consistency_level="Strong"
            )
            tt = time.time() - t0
            log.info(f"assert text match: {tt}")
            assert len(res) >= 0
