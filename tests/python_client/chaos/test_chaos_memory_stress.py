import threading
from time import sleep, time

import pytest
import datetime

from pymilvus import connections
from base.collection_wrapper import ApiCollectionWrapper
from chaos.checker import Op, CreateChecker, InsertFlushChecker, IndexChecker, SearchChecker, QueryChecker
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common import common_func as cf
from common import common_type as ct
from chaos import chaos_commons as cc
from chaos.chaos_commons import gen_experiment_config, get_chaos_yamls, start_monitor_threads
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
        target: explore query node behavior after memory stress chaos injected and recovered
        method: 1. create a collection, insert some data
                2. inject memory stress chaos
                3. load collection and search, query
                4. todo (verify query node response)
                5. delete chaos or chaos finished
                6. release and reload collection, verify search and query is available
        expected: after chaos deleted, load, search and query are all available
        """
        c_name = 'chaos_memory_nx6DNW4q'
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(c_name)
        log.debug(collection_w.schema)
        log.debug(collection_w._shards_num)

        # apply memory stress
        # apply_memory_stress(chaos_yaml)

        # wait memory stress
        # sleep(constants.WAIT_PER_OP * 2)

        # query
        collection_w.release()
        collection_w.load()
        term_expr = f'{ct.default_int64_field_name} in [0, 1, 999, 99]'
        for i in range(4):
            t0_query = datetime.datetime.now()
            query_res, _ = collection_w.query(term_expr)
            tt_query = datetime.datetime.now() - t0_query
            log.info(f"{i} query cost: {tt_query}")
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
            log.info(f'After {i + 1} insert, num_entities: {collection_w.num_entities}')
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
        """
        target: test inject memory stress into indexnode
        method: 1.Deploy milvus and limit indexnode memory resource 1Gi
                2.Create collection and insert some data
                3.Create index
                4.Inject memory stress chaos 512Mi
        expected:
        """
        # init collection and insert 250 nb
        # nb = 50000  # vector size: 512*4*nb about 100Mi and create index need 600Mi memory
        nb = 256000
        dim = 512
        # c_name = cf.gen_unique_str('chaos_memory')
        c_name = 'chaos_memory_gKs8aSUu'
        index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 128}}

        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name,
                                     schema=cf.gen_default_collection_schema(dim=dim), shards_num=1)

        # insert 256000 512 dim entities, size 512Mi
        for i in range(2):
            t0_insert = datetime.datetime.now()
            df = cf.gen_default_dataframe_data(nb=nb // 2, dim=dim)
            res = collection_w.insert(df)[0]
            assert res.insert_count == nb // 2
            # log.info(f'After {i + 1} insert, num_entities: {collection_w.num_entities}')
            tt_insert = datetime.datetime.now() - t0_insert
            log.info(f"{i} insert data cost: {tt_insert}")

        # flush
        t0_flush = datetime.datetime.now()
        assert collection_w.num_entities == nb
        tt_flush = datetime.datetime.now() - t0_flush
        log.info(f'flush {nb * 10} entities cost: {tt_flush}')

        # create index
        t0_index = datetime.datetime.now()
        index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                             index_params=index_params)
        tt_index = datetime.datetime.now() - t0_index

        log.info(f"create index cost: {tt_index}")
        log.info(collection_w.indexes)

        # indexNode start build index, inject chaos memory stress
        chaos_config = gen_experiment_config(chaos_yaml)
        log.debug(chaos_config)
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.debug("inject chaos")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('chaos_yaml', cc.get_chaos_yamls())
    def test_chaos_memory_stress_etcd(self, chaos_yaml):
        """
        target: test inject memory stress into all etcd pods
        method: 1.Deploy milvus and limit etcd memory resource 1Gi witl all mode
                2.Continuously and concurrently do milvus operations
                3.Inject memory stress chaos 51024Mi
                4.After duration, delete chaos stress
        expected: Verify milvus operation succ rate
        """
        mic_checkers = {
            Op.create: CreateChecker(),
            Op.insert: InsertFlushChecker(),
            Op.flush: InsertFlushChecker(flush=True),
            Op.index: IndexChecker(),
            Op.search: SearchChecker(),
            Op.query: QueryChecker()
        }
        # start thread keep running milvus op
        start_monitor_threads(mic_checkers)

        # parse chaos object
        chaos_config = cc.gen_experiment_config(chaos_yaml)
        # duration = chaos_config["spec"]["duration"]
        meta_name = chaos_config.get('metadata').get('name')
        duration = chaos_config.get('spec').get('duration')

        # apply chaos object
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.info("Chaos injected")

        # convert string duration time to a int number in seconds
        if isinstance(duration, str):
            duration = duration.replace('h', '*3600+').replace('m', '*60+').replace('s', '*1+')+'+0'
        else:
            log.error("Duration must be string type")

        # Delete experiment after it's over
        timer = threading.Timer(interval=eval(duration), function=chaos_res.delete, args=(meta_name, False))
        timer.start()
        timer.join()

        # output milvus op succ rate
        for k, ch in mic_checkers.items():
            log.debug(f'Succ rate of {k.value}: {ch.succ_rate()}')
            assert ch.succ_rate() == 1.0
