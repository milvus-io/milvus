# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from operator import methodcaller

from kubernetes import client, config
from milvus_benchmark import config as cf

logger = logging.getLogger("milvus_benchmark.chaos.utils")


def list_pod_for_namespace(label_selector="app.kubernetes.io/instance=zong-standalone"):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_namespaced_pod(namespace=cf.NAMESPACE, label_selector=label_selector)
    pods = []
    # label_selector = 'release=zong-single'
    for i in ret.items:
        pods.append(i.metadata.name)
        # print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
    return pods


def assert_fail(func, milvus_client, **params):
    try:
        methodcaller(func, **params)(milvus_client)
    except Exception as e:
        logger.info(str(e))
        pass
    else:
        raise Exception("fail-assert failed")


def assert_pass(func, milvus_client, **params):
    try:
        methodcaller(func, **params)(milvus_client)
        logger.debug("&&&&&&&&&&&&&&&&&&&&")
    except Exception as e:
        raise
