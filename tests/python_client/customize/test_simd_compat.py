import pytest
import time

from pymilvus import connections
from utils.util_log import test_log as log
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct
from milvus_operator import MilvusOperator
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel

supported_simd_types = ["sse4_2", "avx", "avx2", "avx512"]


# TODO: implement simd config after supported
@pytest.mark.skip(reason='simd config is not supported yet')
class TestSimdCompatibility:
    """
    steps
    1. [test_milvus_install]: set up milvus with customized simd configured
    2. [test_simd_compat_e2e]: verify milvus is working well
    4. [test_milvus_cleanup]: clear the env  "avx", "avx2", "avx512"
    """

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('simd', [
        pytest.param("sse4_2", marks=pytest.mark.dependency(name='ins_sse4_2')),
        # pytest.param("avx", marks=pytest.mark.dependency(name='ins_avx')),
        # pytest.param("avx2", marks=pytest.mark.dependency(name='ins_avx2')),
        pytest.param("avx512", marks=pytest.mark.dependency(name='ins_avx512'))
    ])
    def test_milvus_install(self, request, simd):
        release_name = "mil-simd-" + cf.gen_digits_by_length(6)
        namespace = 'chaos-testing'
        cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-latest',
                       'metadata.namespace': namespace,
                       'metadata.name': release_name,
                       'spec.components.proxy.serviceType': 'LoadBalancer',
                       # TODO: use simd config instead of replicas
                       'spec.components.queryNode.replicas': 2
                       }
        milvus_op = MilvusOperator()
        milvus_op.install(cus_configs)
        healthy = milvus_op.wait_for_healthy(release_name, namespace)
        log.info(f"milvus healthy: {healthy}")
        assert healthy
        endpoint = milvus_op.endpoint(release_name, namespace)
        log.info(f"milvus endpoint: {endpoint}")
        host = endpoint.split(':')[0]
        port = endpoint.split(':')[1]
        conn = connections.connect(simd, host=host, port=port)
        assert conn is not None
        mil = MilvusSys(alias=simd)
        log.info(f"milvus build version: {mil.build_version}")
        # TODO: Verify simd config instead of replicas
        assert len(mil.query_nodes) == 2

        # cache results for dependent tests
        cache = {'release_name': release_name,
                 'namespace': namespace,
                 'alias': simd,
                 'simd': simd
                 }
        request.config.cache.set(simd, cache)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('simd', [
        pytest.param("sse4_2", marks=pytest.mark.dependency(name='e2e_sse4_2', depends=["ins_sse4_2"])),
        # pytest.param("avx", marks=pytest.mark.dependency(name='e2e_avx', depends=["ins_avx"])),
        # pytest.param("avx2", marks=pytest.mark.dependency(name='e2e_avx2', depends=["ins_avx2"])),
        pytest.param("avx512", marks=pytest.mark.dependency(name='e2e_avx512', depends=["ins_avx512"]))
    ])
    def test_simd_compat_e2e(self, request, simd):
        log.info(f"start to e2e verification: {simd}")
        # parse results from previous results
        results = request.config.cache.get(simd, None)
        alias = results.get('alias', simd)
        conn = connections.connect(alias=alias)
        assert conn is not None
        simd_cache = request.config.cache.get(simd, None)
        log.info(f"simd_cache: {simd_cache}")
        # create
        name = cf.gen_unique_str("compat")
        t0 = time.time()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=name,
                                     schema=cf.gen_default_collection_schema(),
                                     using=alias,
                                     timeout=40)
        tt = time.time() - t0
        assert collection_w.name == name
        entities = collection_w.num_entities
        log.info(f"assert create collection: {tt}, init_entities: {entities}")

        # insert
        data = cf.gen_default_list_data()
        t0 = time.time()
        _, res = collection_w.insert(data)
        tt = time.time() - t0
        log.info(f"assert insert: {tt}")
        assert res

        # flush
        t0 = time.time()
        assert collection_w.num_entities == len(data[0]) + entities
        tt = time.time() - t0
        entities = collection_w.num_entities
        log.info(f"assert flush: {tt}, entities: {entities}")

        # search
        collection_w.load()
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res_1) == 1
        collection_w.release()

        # index
        d = cf.gen_default_list_data()
        collection_w.insert(d)
        log.info(f"assert index entities: {collection_w.num_entities}")
        _index_params = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
        t0 = time.time()
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=_index_params,
                                             name=cf.gen_unique_str())
        tt = time.time() - t0
        log.info(f"assert index: {tt}")
        assert len(collection_w.indexes) == 1

        # search
        t0 = time.time()
        collection_w.load()
        tt = time.time() - t0
        log.info(f"assert load: {tt}")
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")

        # query
        term_expr = f'{ct.default_int64_field_name} in [1001,1201,4999,2999]'
        t0 = time.time()
        res, _ = collection_w.query(term_expr)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('simd', [
        pytest.param("sse4_2", marks=pytest.mark.dependency(name='clear_sse4_2', depends=["ins_sse4_2", "e2e_sse4_2"])),
        # pytest.param("avx", marks=pytest.mark.dependency(name='clear_avx', depends=["ins_avx", "e2e_avx"])),
        # pytest.param("avx2", marks=pytest.mark.dependency(name='clear_avx2', depends=["ins_avx2", "e2e_avx2"])),
        pytest.param("avx512", marks=pytest.mark.dependency(name='clear_avx512', depends=["ins_avx512", "e2e_avx512"]))
    ])
    def test_milvus_cleanup(self, request, simd):
        # get release name from previous results
        results = request.config.cache.get(simd, None)
        release_name = results.get('release_name', "name-not-found")
        namespace = results.get('namespace', "namespace-not-found")
        simd_cache = request.config.cache.get(simd, None)
        log.info(f"stat to cleanup: {simd}")
        log.info(f"simd_cache: {simd_cache}")
        log.info(f"release_name: {release_name}")
        log.info(f"namespace: {namespace}")

        milvus_op = MilvusOperator()
        milvus_op.uninstall(release_name, namespace)
