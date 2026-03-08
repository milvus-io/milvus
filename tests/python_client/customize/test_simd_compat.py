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

# sorted by the priority order of the simd
# | configuration | possible returned SIMD |
# |--------|----------|
# | auto | avx512 / avx2 / sse4_2|
# | avx512 | avx512 / avx2 / sse4_2|
# | avx2 | avx2 / sse4_2|
# | avx | sse4_2|
# | sse4_2 | sse4_2|
supported_simd_types = ["avx512", "avx2", "avx", "sse4_2"]
namespace = 'chaos-testing'


def _install_milvus(simd):
    release_name = f"mil-{simd.replace('_','-')}-" + cf.gen_digits_by_length(6)
    cus_configs = {'spec.components.image': 'harbor.milvus.io/milvus/milvus:master-latest',
                   'metadata.namespace': namespace,
                   'metadata.name': release_name,
                   'spec.config.common.simdType': simd
                   }
    milvus_op = MilvusOperator()
    log.info(f"install milvus with configs: {cus_configs}")
    milvus_op.install(cus_configs)
    healthy = milvus_op.wait_for_healthy(release_name, namespace, timeout=1200)
    log.info(f"milvus healthy: {healthy}")
    if healthy:
        endpoint = milvus_op.endpoint(release_name, namespace).split(':')
        log.info(f"milvus endpoint: {endpoint}")
        host = endpoint[0]
        port = endpoint[1]
        return release_name, host, port
    else:
        return release_name, None, None


class TestSimdCompatibility:

    def teardown_method(self):
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('simd', supported_simd_types)
    def test_simd_compat_e2e(self, simd):
        """
       steps
       1. [test_milvus_install]: set up milvus with customized simd configured
       2. [test_simd_compat_e2e]: verify milvus is working well
       4. [test_milvus_cleanup]: delete milvus instances in teardown
       """
        log.info(f"start to install milvus with simd {simd}")
        release_name, host, port = _install_milvus(simd)
        time.sleep(10)
        self.release_name = release_name
        assert host is not None
        conn = connections.connect("default", host=host, port=port)
        assert conn is not None
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        log.info(f"milvus simdType: {mil.simd_type}")
        assert str(mil.simd_type).lower() == simd.lower()

        log.info(f"start to e2e verification: {simd}")
        # create
        prefix = "simd_"
        name = cf.gen_unique_str(prefix)
        t0 = time.time()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection_wrap(name=name)
        tt = time.time() - t0
        assert collection_w.name == name
        entities = collection_w.num_entities
        log.info(f"assert create collection: {tt}, init_entities: {entities}")

        # insert
        for _ in range(10):
            data = cf.gen_default_list_data(nb=300)
            t0 = time.time()
            _, res = collection_w.insert(data)
            tt = time.time() - t0
            log.info(f"assert insert: {tt}")
            assert res

        # flush
        t0 = time.time()
        _, check_result = collection_w.flush(timeout=180)
        assert check_result
        assert collection_w.num_entities == len(data[0]) + entities
        tt = time.time() - t0
        entities = collection_w.num_entities
        log.info(f"assert flush: {tt}, entities: {entities}")

        # index
        index_params = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
        t0 = time.time()
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=index_params,
                                             index_name=cf.gen_unique_str())
        index, _ = collection_w.create_index(field_name=ct.default_string_field_name,
                                             index_params={},
                                             index_name=cf.gen_unique_str())
        tt = time.time() - t0
        log.info(f"assert index: {tt}")
        assert len(collection_w.indexes) == 2

        # load
        collection_w.load()

        # search
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res_1) == 1

        # release
        collection_w.release()

        # insert
        d = cf.gen_default_list_data()
        collection_w.insert(d)

        # search
        t0 = time.time()
        collection_w.load()
        tt = time.time() - t0
        log.info(f"assert load: {tt}")
        nq = 5
        topk = 5
        search_vectors = cf.gen_vectors(nq, ct.default_dim)
        t0 = time.time()
        res, _ = collection_w.search(data=search_vectors,
                                     anns_field=ct.default_float_vec_field_name,
                                     param=search_params, limit=topk)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")
        assert len(res) == nq
        assert len(res[0]) <= topk
        # query
        term_expr = f'{ct.default_int64_field_name} in [1, 2, 3, 4]'
        t0 = time.time()
        res, _ = collection_w.query(term_expr)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")
        assert len(res) >= 4

