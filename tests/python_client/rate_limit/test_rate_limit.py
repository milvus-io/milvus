import multiprocessing
import numbers
import random

import pytest
import pandas as pd
from time import sleep

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from utils.util_k8s import wait_pods_ready, read_pod_log
from utils.util_pymilvus import get_latest_tag
from common.constants import *
from customize.milvus_operator import MilvusOperator


prefix = "rate_limit_collection"
exp_name = "name"
exp_schema = "schema"
TIMEOUT = 1800
milvus_port = "19530"
default_schema = cf.gen_default_collection_schema()
IMAGE_REPOSITORY = ct.IMAGE_REPOSITORY_MILVUS
NAMESPACE = ct.NAMESPACE_CHAOS_TESTING
rate_limit_period = 61


class TestRateLimit(TestcaseBase):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The followings are valid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("MILVUS_MODE", ["standalone", "cluster"])
    @pytest.mark.parametrize("rate_limit_enable", ["true", "false"])
    @pytest.mark.parametrize("collection_rate_limit", ["10", "500"])
    def test_rate_limit_create_drop_collection(self, MILVUS_MODE, rate_limit_enable, collection_rate_limit):
        """
        target: test rate limit for ddl (create collection)
        method: 1. install milvus with different rate limit parameters
                2. create/drop collectionRateLimit+1 collections
        expected: 1. raise exception with rate limit enabled in one rate limit period
                  2. create/drop collections successfully with rate limit disabled
                  3. create/drop collections successfully in next rate limit period
        """
        # 1. install milvus with operator
        release_name = "rate_limit" + MILVUS_MODE + rate_limit_enable + collection_rate_limit
        image_tag = get_latest_tag()
        image = f'{IMAGE_REPOSITORY}:{image_tag}'
        host = cf.install_milvus_operator_specific_config(NAMESPACE, MILVUS_MODE, release_name, image,
                                                          rate_limit_enable, collection_rate_limit)

        # 2. connect milvus
        self.connection_wrap.add_connection(default={"host": host, "port": milvus_port})
        self.connection_wrap.connect(alias='default')

        # 3. create maximum numbers of collections in one two limit periods
        for period in range(2):
            log.info("test_rate_limit_create_collection: starting to check rate limit period %d" % (period+1))
            for collection_num in range(collectionRateLimit):
                log.info("test_rate_limit_create_collection: creating collection %d" % (collection_num+1))
                c_name = cf.gen_unique_str(prefix)
                collection_w = self.init_collection_wrap(c_name,
                                                         check_task=CheckTasks.check_collection_property,
                                                         check_items={exp_name: c_name, exp_schema: default_schema})
        # 4. create one more collection
            log.info("test_rate_limit_create_collection: creating one more collection")
            c_name = cf.gen_unique_str(prefix)
            error = {ct.err_code: 0, ct.err_msg: "Fail to create collection"}
            if rate_limit_enable:
                collection_w = self.init_collection_wrap(c_name, check_task=CheckTasks.err_res, check_items=error)
                # 5. sleep 61s to verify create collections in next rate limit period
                sleep(rate_limit_period)
        # 6. drop maximum+1 numbers of collections in two rate limit period
        collection_list = self.utility_wrap.list_collections()[0]
        drop_num = 0
        error = {ct.err_code: 0, ct.err_msg: "Fail to drop collection"}
        for collection_object in self.collection_object_list:
            if collection_object.collection is not None and collection_object.name in collection_list:
                log.info("test_rate_limit_create_collection: dropping collection %d" % (drop_num + 1))
                if drop_num <= collection_rate_limit:
                    collection_list.remove(collection_object.name)
                    collection_object.drop()
                    drop_num += 1
                else:
                    if rate_limit_enable:
                        collection_object.drop(check_task=CheckTasks.err_res, check_items=error)
                        # 7. sleep 61s to verify drop collections in next rate limit period
                        sleep(rate_limit_period)
                    drop_num = 0
        # 8. export milvus logs
        label = f"app.kubernetes.io/instance={release_name}"
        log.info('Start to export milvus pod logs')
        read_pod_log(namespace=NAMESPACE, label_selector=label, release_name=release_name)

        # 9. uninstall milvus
        mil.uninstall(release_name, namespace=NAMESPACE)


