import datetime

import pytest
from pymilvus import connections, MilvusException

from base.collection_wrapper import ApiCollectionWrapper
from common.common_type import CaseLabel
from customize.milvus_operator import MilvusOperator
from scale import constants
from common import common_func as cf
from common import common_type as ct
from utils.util_k8s import read_pod_log, wait_pods_ready
from utils.util_log import test_log as log
from utils.util_pymilvus import get_latest_tag

nb = 10000
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 128}}


class TestIndexNodeScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_expand_index_node(self):
        """
        target: test expand indexNode from 1 to 2
        method: 1.deploy two indexNode
                2.create index with two indexNode
                3.expand indexNode from 1 to 2
                4.create index with one indexNode
        expected: The cost of one indexNode is about twice that of two indexNodes
        """
        release_name = "expand-index"
        image_tag = get_latest_tag()
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        init_replicas = 1
        expand_replicas = 2
        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'spec.mode': 'cluster',
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.indexNode.replicas': init_replicas,
            'spec.components.dataNode.replicas': 2,
            'spec.config.common.retentionDuration': 60
        }
        mic = MilvusOperator()
        mic.install(data_config)
        if mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1800):
            host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        else:
            # If deploy failed and want to uninsatll mic
            # log.warning(f'Deploy {release_name} timeout and ready to uninstall')
            # mic.uninstall(release_name, namespace=constants.NAMESPACE)
            raise MilvusException(message=f'Milvus healthy timeout 1800s')

        try:
            # connect
            connections.add_connection(default={"host": host, "port": 19530})
            connections.connect(alias='default')

            # create collection
            c_name = "index_scale_one"
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())

            # insert data
            data = cf.gen_default_dataframe_data(nb)
            loop = 100
            for i in range(loop):
                collection_w.insert(data, timeout=60)
            assert collection_w.num_entities == nb * loop

            # create index
            # Note that the num of segments and the num of indexNode are related to indexing time
            start = datetime.datetime.now()
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params, timeout=60)
            assert collection_w.has_index()[0]
            t0 = datetime.datetime.now() - start
            log.info(f'Create index on {init_replicas} indexNode cost t0: {t0}')

            # drop index
            collection_w.drop_index()
            assert not collection_w.has_index()[0]

            # expand indexNode
            mic.upgrade(release_name, {'spec.components.indexNode.replicas': expand_replicas}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            # create index again
            start = datetime.datetime.now()
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params, timeout=60)
            assert collection_w.has_index()[0]
            t1 = datetime.datetime.now() - start
            log.info(f'Create index on {expand_replicas} indexNode cost t1: {t1}')
            collection_w.drop_index()

            start = datetime.datetime.now()
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params, timeout=60)
            assert collection_w.has_index()[0]
            t2 = datetime.datetime.now() - start
            log.info(f'Create index on {expand_replicas} indexNode cost t2: {t2}')

            log.debug(f't2 is {t2}, t0 is {t0}, t0/t2 is {t0 / t2}')
            # assert round(t0 / t2) == 2

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)

    @pytest.mark.tags(CaseLabel.L3)
    def test_shrink_index_node(self):
        """
        target: test shrink indexNode from 2 to 1
        method: 1.deploy two indexNode
                2.create index with two indexNode
                3.shrink indexNode from 2 to 1
                4.create index with 1 indexNode
        expected: The cost of one indexNode is about twice that of two indexNodes
        """
        release_name = "shrink-index"
        image_tag = get_latest_tag()
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.mode': 'cluster',
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.indexNode.replicas': 2,
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
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params, timeout=60)
            assert collection_w.has_index()[0]
            t0 = datetime.datetime.now() - start

            log.info(f'Create index on 2 indexNode cost t0: {t0}')

            collection_w.drop_index()
            assert not collection_w.has_index()[0]

            # shrink indexNode from 2 to 1
            mic.upgrade(release_name, {'spec.components.indexNode.replicas': 1}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            start = datetime.datetime.now()
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params, timeout=60)
            assert collection_w.has_index()[0]
            t1 = datetime.datetime.now() - start
            log.info(f'Create index on 1 indexNode cost t1: {t1}')
            collection_w.drop_index()
            assert not collection_w.has_index()[0]

            start = datetime.datetime.now()
            collection_w.create_index(ct.default_float_vec_field_name, default_index_params, timeout=60)
            assert collection_w.has_index()[0]
            t2 = datetime.datetime.now() - start
            log.info(f'Create index on 1 indexNode cost t2: {t2}')

            log.debug(f'one indexNode: {t2}')
            log.debug(f't2 is {t2}, t0 is {t0}, t2/t0 is {t2 / t0}')
            # assert round(t2 / t0) == 2

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)
