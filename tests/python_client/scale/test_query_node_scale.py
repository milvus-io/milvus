import random
import threading
import time

import pytest

from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from common.common_type import CaseLabel, CheckTasks
from customize.milvus_operator import MilvusOperator
from common import common_func as cf
from common import common_type as ct
from scale import constants, scale_common
from pymilvus import Index, connections, MilvusException
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, read_pod_log
from utils.util_pymilvus import get_latest_tag
from utils.wrapper import counter

nb = 5000
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
tag_prefix = '2.1.0'
tag_latest = '2.1.0-latest'


@pytest.mark.tags(CaseLabel.L3)
class TestQueryNodeScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_scale_query_node(self):
        """
        target: test scale queryNode
        method: 1.deploy milvus cluster with 1 queryNode
                2.prepare work (connect, create, insert, index and load)
                3.continuously search (daemon thread)
                4.expand queryNode from 2 to 5
                5.continuously insert new data (daemon thread)
                6.shrink queryNode from 5 to 3
        expected: Verify milvus remains healthy and search successfully during scale
        """
        release_name = "scale-query"
        image_tag = get_latest_tag(tag_prefix=tag_prefix, tag_latest=tag_latest)
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        query_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.queryNode.replicas': 1,
            'spec.config.dataCoord.enableCompaction': True,
            'spec.config.dataCoord.enableGarbageCollection': True
        }
        mic = MilvusOperator()
        mic.install(query_config)
        if mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1800):
            host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        else:
            raise MilvusException(message=f'Milvus healthy timeout 1800s')

        try:
            # connect
            connections.add_connection(default={"host": host, "port": 19530})
            connections.connect(alias='default')

            # create
            c_name = cf.gen_unique_str("scale_query")
            # c_name = 'scale_query_DymS7kI4'
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema(), shards_num=2)

            # insert two segments
            for i in range(3):
                df = cf.gen_default_dataframe_data(nb)
                collection_w.insert(df)
                log.debug(collection_w.num_entities)

            # create index
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
            assert collection_w.has_index()[0]
            assert collection_w.index()[0] == Index(collection_w.collection, ct.default_float_vec_field_name,
                                                    default_index_params)

            # load
            collection_w.load()

            # scale queryNode to 5
            mic.upgrade(release_name, {'spec.components.queryNode.replicas': 5}, constants.NAMESPACE)

            @counter
            def do_search():
                """ do search """
                search_res, is_succ = collection_w.search(cf.gen_vectors(1, ct.default_dim),
                                                          ct.default_float_vec_field_name, ct.default_search_params,
                                                          ct.default_limit, check_task=CheckTasks.check_nothing)
                assert len(search_res) == 1
                return search_res, is_succ

            def loop_search():
                """ continuously search """
                while True:
                    do_search()

            threading.Thread(target=loop_search, args=(), daemon=True).start()

            # wait new QN running, continuously insert
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            @counter
            def do_insert():
                """ do insert """
                return collection_w.insert(cf.gen_default_dataframe_data(1000), check_task=CheckTasks.check_nothing)

            def loop_insert():
                """ loop insert """
                while True:
                    do_insert()

            threading.Thread(target=loop_insert, args=(), daemon=True).start()

            log.debug(collection_w.num_entities)
            time.sleep(20)
            log.debug("Expand querynode test finished")

            mic.upgrade(release_name, {'spec.components.queryNode.replicas': 3}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            log.debug(collection_w.num_entities)
            time.sleep(60)
            scale_common.check_succ_rate(do_search)
            scale_common.check_succ_rate(do_insert)
            log.debug("Shrink querynode test finished")

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)

    def test_scale_query_node_replicas(self):
        """
        target: test scale out querynode when load multi replicas
        method: 1.Deploy cluster with 5 querynodes
                2.Create collection with 2 shards
                3.Insert 10 segments and flushed
                4.Load collection with 2 replicas
                5.Scale out querynode from 5 to 6 while search and insert growing data
        expected: Verify search succ rate is 100%
        """
        release_name = "scale-replica"
        image_tag = get_latest_tag(tag_prefix=tag_prefix, tag_latest=tag_latest)
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        query_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.queryNode.replicas': 5,
            'spec.config.dataCoord.enableCompaction': True,
            'spec.config.dataCoord.enableGarbageCollection': True
        }
        mic = MilvusOperator()
        mic.install(query_config)
        if mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1800):
            host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        else:
            raise MilvusException(message=f'Milvus healthy timeout 1800s')

        try:
            scale_querynode = random.choice([6, 7, 4, 3])
            connections.connect("scale-replica", host=host, port=19530)

            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=cf.gen_unique_str("scale_out"), schema=cf.gen_default_collection_schema(), using='scale-replica')

            # insert 10 sealed segments
            for i in range(5):
                df = cf.gen_default_dataframe_data(start=i * ct.default_nb)
                collection_w.insert(df)
                assert collection_w.num_entities == (i + 1) * ct.default_nb

            collection_w.load(replica_number=2)

            @counter
            def do_search():
                """ do search """
                search_res, is_succ = collection_w.search(cf.gen_vectors(1, ct.default_dim),
                                                          ct.default_float_vec_field_name, ct.default_search_params,
                                                          ct.default_limit, check_task=CheckTasks.check_nothing)
                assert len(search_res) == 1
                return search_res, is_succ

            def loop_search():
                """ continuously search """
                while True:
                    do_search()

            threading.Thread(target=loop_search, args=(), daemon=True).start()

            # scale out
            mic.upgrade(release_name, {'spec.components.queryNode.replicas': scale_querynode}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")
            log.debug("Scale out querynode success")

            time.sleep(100)
            scale_common.check_succ_rate(do_search)
            log.debug("Scale out test finished")

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)

    def test_scale_in_query_node_less_than_replicas(self):
        """
        target: test scale in cluster and querynode < replica
        method: 1.Deploy cluster with 3 querynodes
                2.Create and insert data, flush
                3.Load collection with 2 replica number
                4.Scale in querynode from 3 to 1 and query
                5.Scale out querynode from 1 back to 3
        expected: Verify search successfully after scale out
        """
        release_name = "scale-in-query"
        image_tag = get_latest_tag(tag_prefix=tag_prefix, tag_latest=tag_latest)
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        query_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.queryNode.replicas': 2,
            'spec.config.dataCoord.enableCompaction': True,
            'spec.config.dataCoord.enableGarbageCollection': True
        }
        mic = MilvusOperator()
        mic.install(query_config)
        if mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1800):
            host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        else:
            raise MilvusException(message=f'Milvus healthy timeout 1800s')
        try:
            # prepare collection
            connections.connect("scale-in", host=host, port=19530)
            utility_w = ApiUtilityWrapper()
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=cf.gen_unique_str("scale_in"), schema=cf.gen_default_collection_schema(), using="scale-in")
            collection_w.insert(cf.gen_default_dataframe_data())
            assert collection_w.num_entities == ct.default_nb

            # load multi replicas and search success
            collection_w.load(replica_number=2)
            search_res, is_succ = collection_w.search(cf.gen_vectors(1, ct.default_dim),
                                                      ct.default_float_vec_field_name,
                                                      ct.default_search_params, ct.default_limit)
            assert len(search_res[0].ids) == ct.default_limit
            log.info("Search successfully after load with 2 replicas")
            log.debug(collection_w.get_replicas()[0])
            log.debug(utility_w.get_query_segment_info(collection_w.name, using="scale-in"))

            # scale in querynode from 2 to 1, less than replica number
            log.debug("Scale in querynode from 2 to 1")
            mic.upgrade(release_name, {'spec.components.queryNode.replicas': 1}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            # search and not assure success
            collection_w.search(cf.gen_vectors(1, ct.default_dim),
                                ct.default_float_vec_field_name, ct.default_search_params,
                                ct.default_limit, check_task=CheckTasks.check_nothing)
            log.debug(collection_w.get_replicas(check_task=CheckTasks.check_nothing)[0])

            # scale querynode from 1 back to 2
            mic.upgrade(release_name, {'spec.components.queryNode.replicas': 2}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            # verify search success
            collection_w.search(cf.gen_vectors(1, ct.default_dim),
                                ct.default_float_vec_field_name, ct.default_search_params, ct.default_limit)
            # Verify replica info is correct
            replicas = collection_w.get_replicas()[0]
            assert len(replicas.groups) == 2
            for group in replicas.groups:
                assert len(group.group_nodes) == 1
            # Verify loaded segment info is correct
            seg_info = utility_w.get_query_segment_info(collection_w.name, using="scale-in")[0]
            num_entities = 0
            for seg in seg_info:
                assert len(seg.nodeIds) == 2
                num_entities += seg.num_rows
            assert num_entities == ct.default_nb

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)
