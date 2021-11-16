import pytest

from base.collection_wrapper import ApiCollectionWrapper
from common.common_type import CaseLabel
from common import common_func as cf
from common import common_type as ct
from scale import constants, scale_common
from scale.helm_env import HelmEnv
from pymilvus import connections, utility

prefix = "data_scale"
default_schema = cf.gen_default_collection_schema()
default_search_exp = "int64 >= 0"
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestDataNodeScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_expand_data_node(self):
        """
        target: test create and insert api after expand dataNode pod
        method: 1.create collection a and insert df
                2.expand dataNode pod from 1 to 2
                3.verify collection a property and verify create and insert of new collection
        expected: two collection create and insert op are both correctly
        """
        release_name = "scale-data"
        milvusOp, host, port = scale_common.deploy_default_milvus(release_name)


        # connect
        connections.add_connection(default={"host": host, "port": port})
        connections.connect(alias='default')
        # create
        c_name = cf.gen_unique_str(prefix)
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())
        # # insert
        data = cf.gen_default_list_data()
        mutation_res, _ = collection_w.insert(data)
        assert mutation_res.insert_count == ct.default_nb
        # scale dataNode to 2 pods
        milvusOp.upgrade(release_name, {'spec.components.dataNode.replicas': 2}, constants.NAMESPACE)
        milvusOp.wait_for_healthy(release_name, constants.NAMESPACE)

        # after scale, assert data consistent
        assert utility.has_collection(c_name)
        assert collection_w.num_entities == ct.default_nb
        # assert new operations
        new_cname = cf.gen_unique_str(prefix)
        new_collection_w = ApiCollectionWrapper()
        new_collection_w.init_collection(name=new_cname, schema=cf.gen_default_collection_schema())
        new_mutation_res, _ = new_collection_w.insert(data)
        assert new_mutation_res.insert_count == ct.default_nb
        assert new_collection_w.num_entities == ct.default_nb
        # assert old collection ddl
        mutation_res_2, _ = collection_w.insert(data)
        assert mutation_res.insert_count == ct.default_nb
        assert collection_w.num_entities == ct.default_nb*2

        collection_w.drop()
        new_collection_w.drop()

        # milvusOp.uninstall(release_name, namespace=constants.NAMESPACE)

    @pytest.mark.tags(CaseLabel.L3)
    def test_shrink_data_node(self):
        """
        target: test shrink dataNode from 2 to 1
        method: 1.create collection and insert df 2. shrink dataNode 3.insert df
        expected: verify the property of collection which channel on shrink pod
        """
        release_name = "scale-data"
        env = HelmEnv(release_name=release_name, dataNode=2)
        host = env.helm_install_cluster_milvus(image_pull_policy=constants.IF_NOT_PRESENT)

        # connect
        connections.add_connection(default={"host": host, "port": 19530})
        connections.connect(alias='default')

        c_name = "data_scale_one"
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())
        mutation_res, _ = collection_w.insert(data)
        assert mutation_res.insert_count == ct.default_nb
        assert collection_w.num_entities == ct.default_nb

        c_name_2 = "data_scale_two"
        collection_w2 = ApiCollectionWrapper()
        collection_w2.init_collection(name=c_name_2, schema=cf.gen_default_collection_schema())
        mutation_res2, _ = collection_w2.insert(data)
        assert mutation_res2.insert_count == ct.default_nb
        assert collection_w2.num_entities == ct.default_nb

        env.helm_upgrade_cluster_milvus(dataNode=1)

        assert collection_w.num_entities == ct.default_nb
        mutation_res2, _ = collection_w2.insert(data)
        assert collection_w2.num_entities == ct.default_nb*2
        collection_w.drop()
        collection_w2.drop()

        # env.helm_uninstall_cluster_milvus()
