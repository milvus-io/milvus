import datetime
# import pdb

from pymilvus_orm import connections

from base.collection_wrapper import ApiCollectionWrapper
from scale.helm_env import HelmEnv
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log

nb = 50000
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 128}}


class TestIndexNodeScale:

    def test_expand_index_node(self):
        """
        target: test expand indexNode from 1 to 2
        method: 1.deploy two indexNode
                2.create index with two indexNode
                3.expand indexNode from 1 to 2
                4.create index with one indexNode
        expected: The cost of one indexNode is about twice that of two indexNodes
        """
        release_name = "scale-index"
        env = HelmEnv(release_name=release_name)
        env.helm_install_cluster_milvus()

        # connect
        connections.add_connection(default={"host": '10.98.0.8', "port": 19530})
        connections.connect(alias='default')

        data = cf.gen_default_dataframe_data(nb)

        # create
        c_name = "index_scale_one"
        collection_w = ApiCollectionWrapper()
        # collection_w.init_collection(name=c_name)
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())
        # insert
        loop = 10
        for i in range(loop):
            collection_w.insert(data)
        assert collection_w.num_entities == nb*loop

        # create index on collection one and two
        start = datetime.datetime.now()
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        t0 = datetime.datetime.now() - start

        log.debug(f't0: {t0}')

        collection_w.drop_index()
        assert not collection_w.has_index()[0]

        # expand indexNode from 1 to 2
        env.helm_upgrade_cluster_milvus(indexNode=2)

        start = datetime.datetime.now()
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        t1 = datetime.datetime.now() - start

        log.debug(f't1: {t1}')
        assert round(t0 / t1) == 2

    def test_shrink_index_node(self):
        """
        target: test shrink indexNode from 2 to 1
        method: 1.deploy two indexNode
                2.create index with two indexNode
                3.shrink indexNode from 2 to 1
                4.create index with 1 indexNode
        expected: The cost of one indexNode is about twice that of two indexNodes
        """
        release_name = "scale-index"
        env = HelmEnv(release_name=release_name, indexNode=2)
        env.helm_install_cluster_milvus()

        # connect
        connections.add_connection(default={"host": '10.98.0.8', "port": 19530})
        connections.connect(alias='default')

        data = cf.gen_default_dataframe_data(nb)

        # create
        c_name = "index_scale_one"
        collection_w = ApiCollectionWrapper()
        # collection_w.init_collection(name=c_name)
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())
        # insert
        loop = 10
        for i in range(loop):
            collection_w.insert(data)
        assert collection_w.num_entities == nb * loop

        # create index on collection one and two
        start = datetime.datetime.now()
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        t0 = datetime.datetime.now() - start

        log.debug(f'two indexNodes: {t0}')

        collection_w.drop_index()
        assert not collection_w.has_index()[0]

        # expand indexNode from 1 to 2
        # pdb.set_trace()
        env.helm_upgrade_cluster_milvus(indexNode=1)

        start = datetime.datetime.now()
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        t1 = datetime.datetime.now() - start

        log.debug(f'one indexNode: {t1}')
        log.debug(t1 / t0)
        assert round(t1 / t0) == 2

