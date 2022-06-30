from time import sleep
from pymilvus import connections, list_collections, utility
from chaos.checker import (CreateChecker, InsertFlushChecker,
                           SearchChecker, QueryChecker, IndexChecker, Op)
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from chaos.chaos_commons import assert_statistic
from common.common_type import CaseLabel
from common import common_func as cf
from chaos import constants
from customize.milvus_operator import MilvusOperator

from delayed_assert import assert_expectations

namespace = "chaos-testing"


def install_milvus(release_name):
    cus_configs = {'spec.components.image': 'milvusdb/milvus:master-20211206-b20a238',
                   'metadata.namespace': namespace,
                   'metadata.name': release_name,
                   'spec.components.proxy.serviceType': 'LoadBalancer'
                   }
    milvus_op = MilvusOperator()
    log.info(f"install milvus with configs: {cus_configs}")
    milvus_op.install(cus_configs)
    healthy = milvus_op.wait_for_healthy(release_name, namespace, timeout=1200)
    log.info(f"milvus healthy: {healthy}")
    if healthy:
        endpoint = milvus_op.endpoint(release_name, namespace).split(':')
        log.info(f"milvus endpoint: {endpoint}")
        host = endpoint[0]
        port = endpoint[1]
        return release_name, host, port
    else:
        return release_name, None, None


def scale_up_milvus(release_name):
    cus_configs = {
                   'spec.components.queryNode.replicas': 2
                  }
    milvus_op = MilvusOperator()
    log.info(f"scale up milvus with configs: {cus_configs}")
    milvus_op.upgrade(release_name, cus_configs, namespace=namespace)
    healthy = milvus_op.wait_for_healthy(release_name, namespace, timeout=1200)
    log.info(f"milvus healthy: {healthy}")
    if healthy:
        endpoint = milvus_op.endpoint(release_name, namespace).split(':')
        log.info(f"milvus endpoint: {endpoint}")
        host = endpoint[0]
        port = endpoint[1]
        return release_name, host, port
    else:
        return release_name, None, None


class TestAutoLoadBalance(object):


    def teardown_method(self):
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)

    def test_auto_load_balance(self):
        """

        """
        log.info(f"start to install milvus")
        release_name, host, port = install_milvus("test-auto-load-balance")  # todo add release name
        self.release_name = release_name
        assert host is not None
        conn = connections.connect("default", host=host, port=port)
        assert conn is not None
        self.health_checkers = {
            Op.create: CreateChecker(),
            Op.insert: InsertFlushChecker(),
            Op.flush: InsertFlushChecker(flush=True),
            Op.index: IndexChecker(),
            Op.search: SearchChecker(),
            Op.query: QueryChecker()
        }
        cc.start_monitor_threads(self.health_checkers)
        # wait  
        sleep(constants.WAIT_PER_OP * 10)
        all_collections = list_collections()
        for c in all_collections:
            seg_info = utility.get_query_segment_info(c)
            seg_distribution = cf.get_segment_distribution(seg_info)
            for k in seg_distribution.keys():
                log.info(f"collection {c}'s segment distribution in node {k} is {seg_distribution[k]['sealed']}")
                log.info(f"collection {c}'s growing segment distribution in node {k} is {seg_distribution[k]['growing']}")
        # first assert
        log.info("first assert")
        assert_statistic(self.health_checkers)

        # scale up
        log.info("scale up milvus")
        scale_up_milvus(self.release_name)
        # reset counting
        cc.reset_counting(self.health_checkers)
        sleep(constants.WAIT_PER_OP * 10)
        all_collections = list_collections()
        for c in all_collections:
            seg_info = utility.get_query_segment_info(c)
            seg_distribution = cf.get_segment_distribution(seg_info)
            for k in seg_distribution.keys():
                log.info(f"collection {c}'s sealed segment distribution in node {k} is {seg_distribution[k]['sealed']}")
                log.info(f"collection {c}'s growing segment distribution in node {k} is {seg_distribution[k]['growing']}")
        # second assert
        log.info("second assert")
        assert_statistic(self.health_checkers)

        # TODO assert segment distribution

        # assert all expectations
        assert_expectations()
