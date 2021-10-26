from time import sleep

import pytest
import datetime

from pymilvus import connections
from base.collection_wrapper import ApiCollectionWrapper
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common import common_func as cf
from common import common_type as ct
from chaos.chaos_commons import gen_experiment_config, get_chaos_yamls
from common.common_type import CaseLabel
from chaos import constants
from utils.util_log import test_log as log


def apply_memory_stress(chaos_yaml):
    chaos_config = gen_experiment_config(chaos_yaml)
    log.debug(chaos_config)
    chaos_res = CusResource(kind=chaos_config['kind'],
                            group=constants.CHAOS_GROUP,
                            version=constants.CHAOS_VERSION,
                            namespace=constants.CHAOS_NAMESPACE)
    chaos_res.create(chaos_config)
    log.debug("chaos injected")


def construct_from_data(collection_name, h5_path='./testdata/random_data_10000.h5'):
    import pandas as pd
    df = pd.read_hdf(h5_path, key='df')
    collection_w = ApiCollectionWrapper()
    collection_w.construct_from_dataframe(collection_name, dataframe=df, primary_field=ct.default_int64_field_name)
    log.debug(collection_w.num_entities)
    return collection_w


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
    def test_chaos_memory_stress_querynode(self, connection, chaos_yaml):
        """
        target: explore querynode behavior after memory stress chaos injected and recovered
        method: 1. create a collection, insert some data
                2. inject memory stress chaos
                3. load collection and search, query
                4. todo (verify querynode response)
                5. delete chaos or chaos finished
                6. release and reload collection, verify search and query is available
        expected: after chaos deleted, load, search and query qre both available
        """
        c_name = cf.gen_unique_str('chaos_memory')
        collection_w = construct_from_data(c_name)
        log.debug(collection_w.schema)

        # apply memory stress
        apply_memory_stress(chaos_yaml)

        # wait memory stress
        sleep(constants.WAIT_PER_OP * 2)

        # query
        collection_w.release()
        collection_w.load()
        term_expr = f'{ct.default_int64_field_name} in [0, 1, 999, 99]'
        t0 = datetime.datetime.now()
        query_res, _ = collection_w.query(term_expr)
        tt = datetime.datetime.now() - t0
        log.info(f"assert query: {tt}")
        assert len(query_res) == 4

        sleep(constants.WAIT_PER_OP * 5)

        # query
        collection_w.release()
        collection_w.load()
        term_expr = f'{ct.default_int64_field_name} in [0, 1, 999, 99]'
        t0 = datetime.datetime.now()
        query_res, _ = collection_w.query(term_expr)
        tt = datetime.datetime.now() - t0
        log.info(f"assert query: {tt}")
        assert len(query_res) == 4

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('chaos_yaml', get_chaos_yamls())
    def test_chaos_memory_stress_datanode(self, chaos_yaml):
        """
        target: test inject memory stress into dataNode
        method: 1.Deploy milvus and limit datanode memory resource
                2.Create collection and insert some data
                3.Inject memory stress chaos
                4.Continue to insert data
        expected:
        """
        # init collection and insert 250 nb
        nb = 25000
        dim = 512
        c_name = cf.gen_unique_str('chaos_memory')
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name,
                                     schema=cf.gen_default_collection_schema(dim=dim))
        for i in range(10):
            t0 = datetime.datetime.now()
            df = cf.gen_default_dataframe_data(nb=nb, dim=dim)
            res = collection_w.insert(df)[0]
            assert res.insert_count == nb
            log.info(f'After {i+1} insert, num_entities: {collection_w.num_entities}')
            tt = datetime.datetime.now() - t0
            log.info(f"{i} insert and flush data cost: {tt}")

        # inject memory stress
        chaos_config = gen_experiment_config(chaos_yaml)
        log.debug(chaos_config)
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.debug("chaos injected")

        # Continue to insert data
        collection_w.insert(df)
        log.info(f'Total num entities: {collection_w.num_entities}')

        # delete chaos
        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_res.delete(metadata_name=meta_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('chaos_yaml', get_chaos_yamls())
    def test_chaos_memory_stress_indexnode(self, connection, chaos_yaml):
        pass
