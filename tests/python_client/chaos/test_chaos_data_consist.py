import pytest
import datetime
from time import sleep

from pymilvus import connections, utility
from base.collection_wrapper import ApiCollectionWrapper
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common import common_func as cf
from common import common_type as ct
from chaos_commons import *
from common.common_type import CaseLabel
from chaos import constants


def reboot_pod(chaos_yaml):
    # parse chaos object
    chaos_config = gen_experiment_config(chaos_yaml)
    log.debug(chaos_config)
    # inject chaos
    chaos_res = CusResource(kind=chaos_config['kind'],
                            group=constants.CHAOS_GROUP,
                            version=constants.CHAOS_VERSION,
                            namespace=constants.CHAOS_NAMESPACE)
    chaos_res.create(chaos_config)
    log.debug("chaos injected")
    sleep(1)
    # delete chaos
    meta_name = chaos_config.get('metadata', None).get('name', None)
    chaos_res.delete(meta_name)
    log.debug("chaos deleted")


class TestChaosData:

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port):
        connections.add_connection(default={"host": host, "port": port})
        conn = connections.connect(alias='default')
        if conn is None:
            raise Exception("no connections")
        return conn

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('chaos_yaml', get_chaos_yamls())
    def test_chaos_data_consist(self, connection, chaos_yaml):
        c_name = cf.gen_unique_str('chaos_collection_')
        nb = 5000
        i_name = cf.gen_unique_str('chaos_index_')
        index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}

        # create
        t0 = datetime.datetime.now()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name,
                                     schema=cf.gen_default_collection_schema())
        tt = datetime.datetime.now() - t0
        log.debug(f"assert create: {tt}")
        assert collection_w.name == c_name

        # insert
        data = cf.gen_default_list_data(nb=nb)
        t0 = datetime.datetime.now()
        _, res = collection_w.insert(data)
        tt = datetime.datetime.now() - t0
        log.debug(f"assert insert: {tt}")
        assert res

        # flush
        t0 = datetime.datetime.now()
        assert collection_w.num_entities == nb
        tt = datetime.datetime.now() - t0
        log.debug(f"assert flush: {tt}")

        # search
        collection_w.load()
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        t0 = datetime.datetime.now()
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        search_res, _ = collection_w.search(data=search_vectors,
                                            anns_field=ct.default_float_vec_field_name,
                                            param=search_params, limit=1)
        tt = datetime.datetime.now() - t0
        log.debug(f"assert search: {tt}")
        assert len(search_res) == 1

        # index
        t0 = datetime.datetime.now()
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=index_params,
                                             name=i_name)
        tt = datetime.datetime.now() - t0
        log.debug(f"assert index: {tt}")
        assert len(collection_w.indexes) == 1

        # query
        term_expr = f'{ct.default_int64_field_name} in [1001,1201,999,99]'
        t0 = datetime.datetime.now()
        query_res, _ = collection_w.query(term_expr)
        tt = datetime.datetime.now() - t0
        log.debug(f"assert query: {tt}")
        assert len(query_res) == 4

        # reboot a pod
        reboot_pod(chaos_yaml)

        # reconnect if needed
        sleep(constants.WAIT_PER_OP * 3)
        reconnect(connections, alias='default')

        # verify collection persists
        assert utility.has_collection(c_name)
        log.debug("assert collection persists")
        collection_w2 = ApiCollectionWrapper()
        collection_w2.init_collection(c_name)
        # verify data persist
        assert collection_w2.num_entities == nb
        log.debug("assert data persists")
        # verify index persists
        assert collection_w2.has_index(i_name)
        log.debug("assert index persists")
        # verify search results persist
        collection_w2.load()
        search_res, _ = collection_w.search(data=search_vectors,
                                            anns_field=ct.default_float_vec_field_name,
                                            param=search_params, limit=1)
        tt = datetime.datetime.now() - t0
        log.debug(f"assert search: {tt}")
        assert len(search_res) == 1
        # verify query results persist
        query_res2, _ = collection_w2.query(term_expr)
        assert len(query_res2) == len(query_res)
        log.debug("assert query result persists")

