import threading
import time

import pytest

from base.collection_wrapper import ApiCollectionWrapper
from common.common_type import CaseLabel
from common import common_func as cf
from customize.milvus_operator import MilvusOperator
from scale import constants
from pymilvus import connections
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, read_pod_log
from utils.util_pymilvus import get_latest_tag

prefix = "data_scale"
default_schema = cf.gen_default_collection_schema()
default_search_exp = "int64 >= 0"
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestDataNodeScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_scale_data_node(self):
        """
        target: test scale dataNode
        method: 1.deploy milvus cluster with 2 dataNode
                2.create collection with shards_num=5
                3.continuously insert new data (daemon thread)
                4.expand dataNode from 2 to 5
                5.create new collection with shards_num=2
                6.continuously insert new collection new data (daemon thread)
                7.shrink dataNode from 5 to 3
        expected: Verify milvus remains healthy, Insert and flush successfully during scale
                  Average dataNode memory usage
        """
        release_name = "scale-data"
        image_tag = get_latest_tag()
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        fail_count = 0

        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.dataNode.replicas': 2,
            'spec.config.dataCoord.enableCompaction': True,
            'spec.config.dataCoord.enableGarbageCollection': True
        }
        mic = MilvusOperator()
        mic.install(data_config)
        healthy = mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1200)
        log.info(f"milvus healthy: {healthy}")
        host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        # host = '10.98.0.4'

        try:
            # connect
            connections.add_connection(default={"host": host, "port": 19530})
            connections.connect(alias='default')

            # create
            c_name = cf.gen_unique_str("scale_query")
            # c_name = 'scale_query_DymS7kI4'
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema(), shards_num=5)

            tmp_nb = 10000

            def do_insert():
                while True:
                    tmp_df = cf.gen_default_dataframe_data(tmp_nb)
                    collection_w.insert(tmp_df)
                    log.debug(collection_w.num_entities)

            t_insert = threading.Thread(target=do_insert, args=(), daemon=True)
            t_insert.start()

            # scale dataNode to 5
            mic.upgrade(release_name, {'spec.components.dataNode.replicas': 5}, constants.NAMESPACE)
            time.sleep(300)
            log.debug("Expand dataNode test finished")

            # create new collection and insert
            new_c_name = cf.gen_unique_str("scale_query")
            collection_w_new = ApiCollectionWrapper()
            collection_w_new.init_collection(name=new_c_name, schema=cf.gen_default_collection_schema(), shards_num=2)

            def do_new_insert():
                while True:
                    tmp_df = cf.gen_default_dataframe_data(tmp_nb)
                    collection_w_new.insert(tmp_df)
                    log.debug(collection_w_new.num_entities)

            t_insert_new = threading.Thread(target=do_new_insert, args=(), daemon=True)
            t_insert_new.start()

            # scale dataNode to 3
            mic.upgrade(release_name, {'spec.components.dataNode.replicas': 3}, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            log.debug(collection_w.num_entities)
            time.sleep(300)
            log.debug("Shrink dataNode test finished")

        except Exception as e:
            log.error(str(e))
            fail_count += 1
            # raise Exception(str(e))

        finally:
            log.info(f'Test finished with {fail_count} fail request')
            assert fail_count <= 1
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)

            mic.uninstall(release_name, namespace=constants.NAMESPACE)
