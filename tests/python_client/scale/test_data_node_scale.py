import threading
import time

import pytest

from base.collection_wrapper import ApiCollectionWrapper
from common.common_type import CaseLabel
from common import common_func as cf
from customize.milvus_operator import MilvusOperator
from scale import constants, scale_common
from pymilvus import connections, MilvusException
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, read_pod_log
from utils.util_pymilvus import get_latest_tag
from utils.wrapper import counter

tag_prefix = '2.1.0'
tag_latest = '2.1.0-latest'


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
        image_tag = get_latest_tag(tag_prefix=tag_prefix, tag_latest=tag_latest)
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'

        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.dataNode.replicas': 2,
            'spec.config.common.retentionDuration': 60
        }
        mic = MilvusOperator()
        mic.install(data_config)
        if mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1800):
            host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        else:
            raise MilvusException(message=f'Milvus healthy timeout 1800s')

        try:
            # connect
            connections.add_connection(default={"host": host, "port": 19530})
            connections.connect(alias='default')

            # create
            c_name = cf.gen_unique_str("scale_data")
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema(), shards_num=4)

            tmp_nb = 10000

            @counter
            def do_insert():
                """ do insert and flush """
                insert_res, is_succ = collection_w.insert(cf.gen_default_dataframe_data(tmp_nb))
                log.debug(collection_w.num_entities)
                return insert_res, is_succ

            def loop_insert():
                """ loop do insert """
                while True:
                    do_insert()

            threading.Thread(target=loop_insert, args=(), daemon=True).start()

            # scale dataNode to 5
            mic.upgrade(release_name, {'spec.components.dataNode.replicas': 5}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")
            log.debug("Expand dataNode test finished")

            # create new collection and insert
            new_c_name = cf.gen_unique_str("scale_data")
            collection_w_new = ApiCollectionWrapper()
            collection_w_new.init_collection(name=new_c_name, schema=cf.gen_default_collection_schema(), shards_num=3)

            @counter
            def do_new_insert():
                """ do new insert """
                insert_res, is_succ = collection_w_new.insert(cf.gen_default_dataframe_data(tmp_nb))
                log.debug(collection_w_new.num_entities)
                return insert_res, is_succ

            def loop_new_insert():
                """ loop new insert """
                while True:
                    do_new_insert()

            threading.Thread(target=loop_new_insert, args=(), daemon=True).start()

            # scale dataNode to 3
            mic.upgrade(release_name, {'spec.components.dataNode.replicas': 3}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            log.debug(collection_w.num_entities)
            time.sleep(300)
            scale_common.check_succ_rate(do_insert)
            scale_common.check_succ_rate(do_new_insert)
            log.debug("Shrink dataNode test finished")

        except Exception as e:
            log.error(str(e))
            # raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)

            mic.uninstall(release_name, namespace=constants.NAMESPACE)
