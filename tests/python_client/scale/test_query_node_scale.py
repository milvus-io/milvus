import pdb
import random

import pytest

from base.collection_wrapper import ApiCollectionWrapper
from common.common_type import CaseLabel
from scale.helm_env import HelmEnv
from common import common_func as cf
from common import common_type as ct
from scale import constants
from pymilvus import Index, connections

prefix = "search_scale"
nb = 5000
nq = 5
default_schema = cf.gen_default_collection_schema()
default_search_exp = "int64 >= 0"
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestQueryNodeScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_expand_query_node(self):
        release_name = "scale-query"
        env = HelmEnv(release_name=release_name)
        host = env.helm_install_cluster_milvus()

        # connect
        connections.add_connection(default={"host": host, "port": 19530})
        connections.connect(alias='default')

        # create
        c_name = "query_scale_one"
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())
        # insert
        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data)
        assert mutation_res.insert_count == ct.default_nb
        # # create index
        # collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        # assert collection_w.has_index()[0]
        # assert collection_w.index()[0] == Index(collection_w.collection, ct.default_float_vec_field_name,
        #                                         default_index_params)
        collection_w.load()
        # vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(5)]
        res1, _ = collection_w.search(data[-1][:5], ct.default_float_vec_field_name,
                                      ct.default_search_params, ct.default_limit)

        # scale queryNode pod
        env.helm_upgrade_cluster_milvus(queryNode=2)

        c_name_2 = "query_scale_two"
        collection_w2 = ApiCollectionWrapper()
        collection_w2.init_collection(name=c_name_2, schema=cf.gen_default_collection_schema())
        collection_w2.insert(data)
        assert collection_w2.num_entities == ct.default_nb
        collection_w2.load()
        res2, _ = collection_w2.search(data[-1][:5], ct.default_float_vec_field_name,
                                       ct.default_search_params, ct.default_limit)

        assert res1[0].ids == res2[0].ids

    @pytest.mark.tags(CaseLabel.L3)
    def test_shrink_query_node(self):
        """
        target: test shrink queryNode from 2 to 1
        method: 1.deploy two queryNode
                2.search two collections in two queryNode
                3.upgrade queryNode from 2 to 1
                4.search second collection
        expected: search result is correct
        """
        # deploy
        release_name = "scale-query"
        env = HelmEnv(release_name=release_name, queryNode=2)
        host = env.helm_install_cluster_milvus(image_pull_policy=constants.IF_NOT_PRESENT)

        # connect
        connections.add_connection(default={"host": host, "port": 19530})
        connections.connect(alias='default')

        # collection one
        data = cf.gen_default_list_data(nb)
        c_name = "query_scale_one"
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        collection_w.load()
        res1, _ = collection_w.search(data[-1][:nq], ct.default_float_vec_field_name,
                                      ct.default_search_params, ct.default_limit)
        assert res1[0].ids[0] == data[0][0]

        # collection two
        c_name_2 = "query_scale_two"
        collection_w2 = ApiCollectionWrapper()
        collection_w2.init_collection(name=c_name_2, schema=cf.gen_default_collection_schema())
        collection_w2.insert(data)
        assert collection_w2.num_entities == nb
        collection_w2.load()
        res2, _ = collection_w2.search(data[-1][:nq], ct.default_float_vec_field_name,
                                       ct.default_search_params, ct.default_limit)
        assert res2[0].ids[0] == data[0][0]

        # scale queryNode pod
        env.helm_upgrade_cluster_milvus(queryNode=1)

        # search
        res1, _ = collection_w.search(data[-1][:nq], ct.default_float_vec_field_name,
                                      ct.default_search_params, ct.default_limit)
        assert res1[0].ids[0] == data[0][0]
        res2, _ = collection_w2.search(data[-1][:nq], ct.default_float_vec_field_name,
                                       ct.default_search_params, ct.default_limit)
        assert res2[0].ids[0] == data[0][0]

        # env.helm_uninstall_cluster_milvus()