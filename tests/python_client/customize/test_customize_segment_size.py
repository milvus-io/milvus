import pytest
import time

from pymilvus import connections
from utils.util_log import test_log as log
from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from common import common_func as cf
from common import common_type as ct
from milvus_operator import MilvusOperator
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel


namespace = 'chaos-testing'


def _install_milvus(seg_size):
    release_name = f"mil-segsize-{seg_size}-" + cf.gen_digits_by_length(6)
    cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-latest',
                   'metadata.namespace': namespace,
                   'metadata.name': release_name,
                   'spec.components.proxy.serviceType': 'LoadBalancer',
                   'spec.config.dataCoord.segment.maxSize': seg_size
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


class TestCustomizeSegmentSize:

    def teardown_method(self):
        pass
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)
        connections.disconnect("default")
        connections.remove_connection("default")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('seg_size, seg_count', [(128, 10), (1024, 2)])
    def test_customize_segment_size(self, seg_size, seg_count):
        """
       steps
       """
        log.info(f"start to install milvus with segment size {seg_size}")
        release_name, host, port = _install_milvus(seg_size)
        self.release_name = release_name
        assert host is not None
        conn = connections.connect("default", host=host, port=port)
        assert conn is not None
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")

        log.info(f"start to e2e verification: {seg_size}")
        # create
        name = cf.gen_unique_str("segsiz")
        t0 = time.time()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=name,
                                     schema=cf.gen_default_collection_schema(),
                                     timeout=40)
        tt = time.time() - t0
        assert collection_w.name == name
        entities = collection_w.num_entities
        log.info(f"assert create collection: {tt}, init_entities: {entities}")

        # insert
        nb = 50000
        data = cf.gen_default_list_data(nb=nb)
        t0 = time.time()
        _, res = collection_w.insert(data)
        tt = time.time() - t0
        log.info(f"assert insert: {tt}")
        assert res
        # insert 2 million entities
        rounds = 40
        for _ in range(rounds-1):
            _, res = collection_w.insert(data)
        entities = collection_w.num_entities
        assert entities == nb * rounds

        # load
        collection_w.load()
        utility_wrap = ApiUtilityWrapper()
        segs, _ = utility_wrap.get_query_segment_info(collection_w.name)
        log.info(f"assert segments: {len(segs)}")
        assert len(segs) == seg_count

        # search
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        t0 = time.time()
        res_1, _ = collection_w.search(data=search_vectors,
                                       anns_field=ct.default_float_vec_field_name,
                                       param=search_params, limit=1, timeout=30)
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
                                             name=cf.gen_unique_str(), timeout=120)
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
                                       param=search_params, limit=1, timeout=30)
        tt = time.time() - t0
        log.info(f"assert search: {tt}")

        # query
        term_expr = f'{ct.default_int64_field_name} in [1001,1201,4999,2999]'
        t0 = time.time()
        res, _ = collection_w.query(term_expr, timeout=30)
        tt = time.time() - t0
        log.info(f"assert query result {len(res)}: {tt}")

