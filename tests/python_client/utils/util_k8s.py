import json
import os.path
import time
import threading
import requests
import etcd3
from pymilvus import connections
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from utils.util_common import find_value_by_key
from common.common_type import in_cluster_env

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def init_k8s_client_config():
    """
    init kubernetes client config
    """
    try:
        in_cluster = os.getenv(in_cluster_env, default='False')
        # log.debug(f"env variable IN_CLUSTER: {in_cluster}")
        if in_cluster.lower() == 'true':
            config.load_incluster_config()
        else:
            config.load_kube_config()
    except Exception as e:
        raise Exception(e)


def get_current_namespace():
    init_k8s_client_config()
    ns = config.list_kube_config_contexts()[1]["context"]["namespace"]
    return ns


def wait_pods_ready(namespace, label_selector, expected_num=None, timeout=360):
    """
    wait pods with label selector all ready

    :param namespace: the namespace where the release
    :type namespace: str

    :param label_selector: labels to restrict which pods are waiting to be ready
    :type label_selector: str

    :param expected_num: expected the minimum number of pods to be ready if not None
    :type expected_num: int

    :param timeout: limits the duration of the call
    :type timeout: int

    :example:
            >>> wait_pods_ready("default", "app.kubernetes.io/instance=scale-query", expected_num=9)
    """
    init_k8s_client_config()
    api_instance = client.CoreV1Api()
    try:
        all_pos_ready_flag = False
        time_cnt = 0
        while not all_pos_ready_flag and time_cnt < timeout:
            api_response = api_instance.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
            all_pos_ready_flag = True
            if expected_num is not None and len(api_response.items) < expected_num:
                all_pos_ready_flag = False
            else:
                for item in api_response.items:
                    if item.status.phase != 'Running':
                        all_pos_ready_flag = False
                        break
                    for c in item.status.container_statuses:
                        log.debug(f"{c.name} status is {c.ready}")
                        if c.ready is False:
                            all_pos_ready_flag = False
                            break
            if not all_pos_ready_flag:
                log.debug("all pods are not ready, please wait")
                time.sleep(30)
                time_cnt += 30
        if all_pos_ready_flag:
            log.info(f"all pods in namespace {namespace} with label {label_selector} are ready")
        else:
            log.info(f"timeout for waiting all pods in namespace {namespace} with label {label_selector} ready")
    except ApiException as e:
        log.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        raise Exception(str(e))

    return all_pos_ready_flag


def get_pod_list(namespace, label_selector):
    """
    get pod list with label selector

    :param namespace: the namespace where the release
    :type namespace: str

    :param label_selector: labels to restrict which pods to list
    :type label_selector: str

    :example:
            >>> get_pod_list("chaos-testing", "app.kubernetes.io/instance=test-proxy-pod-failure, component=proxy")
    """
    init_k8s_client_config()
    api_instance = client.CoreV1Api()
    try:
        api_response = api_instance.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        return api_response.items
    except ApiException as e:
        log.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        raise Exception(str(e))


def get_pod_ip_name_pairs(namespace, label_selector):
    """
    get pod ip name pairs with label selector

    :param namespace: the namespace where the release
    :type namespace: str

    :param label_selector: labels to restrict which pods to list
    :type label_selector: str

    :example:
            >>> get_pod_ip_name_pairs("chaos-testing", "app.kubernetes.io/instance=test-proxy-pod-failure, component=querynode")
    """
    m = dict()
    items = get_pod_list(namespace, label_selector)
    for item in items:
        ip = item.status.pod_ip
        name = item.metadata.name
        m[ip] = name
    return m


def get_querynode_id_pod_pairs(namespace, label_selector):
    """
    get milvus node id and corresponding pod name pairs with label selector

    :param namespace: the namespace where the release
    :type namespace: str

    :param label_selector: labels to restrict which pods to list
    :type label_selector: str

    :example:
            >>> querynode_id_pod_pair = get_querynode_id_pod_pairs("chaos-testing", "app.kubernetes.io/instance=milvus-multi-querynode, component=querynode")
            {
             5: 'milvus-multi-querynode-querynode-7b8f4b5c5-4pn42',
             9: 'milvus-multi-querynode-querynode-7b8f4b5c5-99tx7',
             1: 'milvus-multi-querynode-querynode-7b8f4b5c5-w9sk8',
             3: 'milvus-multi-querynode-querynode-7b8f4b5c5-xx84j',
             6: 'milvus-multi-querynode-querynode-7b8f4b5c5-x95dp'
            }
    """
    # TODO: extend this function to other worker nodes, not only querynode
    querynode_ip_pod_pair = get_pod_ip_name_pairs(namespace, label_selector)
    querynode_id_pod_pair = {}
    ms = MilvusSys()
    for node in ms.query_nodes:
        ip = node["infos"]['hardware_infos']["ip"].split(":")[0]
        querynode_id_pod_pair[node["identifier"]] = querynode_ip_pod_pair[ip]
    return querynode_id_pod_pair


def get_milvus_instance_name(namespace, host="127.0.0.1", port="19530", milvus_sys=None):
    """
    get milvus instance name after connection

    :param namespace: the namespace where the release
    :type namespace: str

    :param host: milvus host ip
    :type host: str

    :param port: milvus port
    :type port: str
    :example:
            >>> milvus_instance_name = get_milvus_instance_name("chaos-testing", "10.96.250.111")
            "milvus-multi-querynode"

    """
    if milvus_sys is None:
        connections.add_connection(_default={"host": host, "port": port})
        connections.connect(alias='_default')
        ms = MilvusSys()
    else:
        ms = milvus_sys
    query_node_ip = ms.query_nodes[0]["infos"]['hardware_infos']["ip"].split(":")[0]
    ip_name_pairs = get_pod_ip_name_pairs(namespace, "app.kubernetes.io/name=milvus")
    pod_name = ip_name_pairs[query_node_ip]
    
    init_k8s_client_config()
    api_instance = client.CoreV1Api()
    try:
        api_response = api_instance.read_namespaced_pod(namespace=namespace, name=pod_name)
    except ApiException as e:
        log.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        raise Exception(str(e))
    milvus_instance_name = api_response.metadata.labels["app.kubernetes.io/instance"]
    return milvus_instance_name


def get_milvus_deploy_tool(namespace, milvus_sys):
    """
    get milvus instance name after connection
    :param namespace: the namespace where the release
    :type namespace: str
    :param milvus_sys: milvus_sys
    :type namespace: MilvusSys
    :example:
            >>> deploy_tool = get_milvus_deploy_tool("chaos-testing", milvus_sys)
            "helm"
    """
    ms = milvus_sys
    query_node_ip = ms.query_nodes[0]["infos"]['hardware_infos']["ip"].split(":")[0]
    ip_name_pairs = get_pod_ip_name_pairs(namespace, "app.kubernetes.io/name=milvus")
    pod_name = ip_name_pairs[query_node_ip]
    init_k8s_client_config()
    api_instance = client.CoreV1Api()
    try:
        api_response = api_instance.read_namespaced_pod(namespace=namespace, name=pod_name)
    except ApiException as e:
        log.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        raise Exception(str(e))
    if ("app.kubernetes.io/managed-by" in api_response.metadata.labels and
        api_response.metadata.labels["app.kubernetes.io/managed-by"] == "milvus-operator"):
        deploy_tool = "milvus-operator"
    else:
        deploy_tool = "helm"
    return deploy_tool


def export_pod_logs(namespace, label_selector, release_name=None):
    """
    export pod logs with label selector to '/tmp/milvus'

    :param namespace: the namespace where the release
    :type namespace: str

    :param label_selector: labels to restrict which pods logs to export
    :type label_selector: str

    :param release_name: use the release name as server logs director name
    :type label_selector: str

    :example:
            >>> export_pod_logs("chaos-testing", "app.kubernetes.io/instance=mic-milvus")
    """
    if isinstance(release_name, str):
        if len(release_name.strip()) == 0:
            raise ValueError("Got an unexpected space release_name")
    else:
        raise TypeError("Got an unexpected non-string release_name")
    pod_log_path = '/tmp/milvus_logs' if release_name is None else f'/tmp/milvus_logs/{release_name}'

    if not os.path.isdir(pod_log_path):
        os.makedirs(pod_log_path)

    # get pods and export logs
    items = get_pod_list(namespace, label_selector=label_selector)
    try:
        for item in items:
            pod_name = item.metadata.name
            os.system(f'kubectl logs {pod_name} > {pod_log_path}/{pod_name}.log 2>&1')
    except Exception as e:
        log.error(f"Exception when export pod {pod_name} logs: %s\n" % e)
        raise Exception(str(e))


def read_pod_log(namespace, label_selector, release_name):
    init_k8s_client_config()
    items = get_pod_list(namespace, label_selector=label_selector)

    try:
        # export log to /tmp/release_name path
        pod_log_path = f'/tmp/milvus_logs/{release_name}'
        if not os.path.isdir(pod_log_path):
            os.makedirs(pod_log_path)

        api_instance = client.CoreV1Api()

        for item in items:
            pod = item.metadata.name
            log.debug(f'Start to read {pod} log')
            logs = api_instance.read_namespaced_pod_log(name=pod, namespace=namespace, async_req=True)
            with open(f'{pod_log_path}/{pod}.log', "w") as f:
                f.write(logs.get())

    except ApiException as e:
        log.error(f"Exception when read pod {pod} logs: %s\n" % e)
        raise Exception(str(e))


def get_metrics_querynode_sq_req_count():
    """ get metric milvus_querynode_collection_num from prometheus"""

    PROMETHEUS = 'http://10.96.7.6:9090'
    query_str = 'milvus_querynode_sq_req_count{app_kubernetes_io_instance="mic-replica",' \
                'app_kubernetes_io_name="milvus",namespace="chaos-testing"}'

    response = requests.get(PROMETHEUS + '/api/v1/query', params={'query': query_str})
    if response.status_code == 200:
        results = response.json()["data"]['result']
        # print(results)
        # print(type(results))
        log.debug(json.dumps(results, indent=4))
        milvus_querynode_sq_req_count = {}
        for res in results:
            if res["metric"]["status"] == "total":
                querynode_id = res["metric"]["node_id"]
                # pod = res["metric"]["pod"]
                value = res["value"][-1]
                milvus_querynode_sq_req_count[int(querynode_id)] = int(value)
        # log.debug(milvus_querynode_sq_req_count)
        return milvus_querynode_sq_req_count
    else:
        raise Exception(-1, f"Failed to get metrics with status code {response.status_code}")


def get_pod_logs(namespace, pod_name):
    init_k8s_client_config()
    api_instance = client.CoreV1Api()
    log.debug(f'Start to read {pod_name} log')
    logs = api_instance.read_namespaced_pod_log(name=pod_name, namespace=namespace, async_req=True)
    return logs


def find_activate_standby_coord_pod(namespace, release_name, coord_type):
    init_k8s_client_config()
    api_instance = client.CoreV1Api()
    etcd_service_name = release_name + "-etcd"
    service = api_instance.read_namespaced_service(name=etcd_service_name, namespace=namespace)
    etcd_cluster_ip = service.spec.cluster_ip
    etcd_port = service.spec.ports[0].port
    etcd = etcd3.client(host=etcd_cluster_ip, port=etcd_port)
    v = etcd.get(f'by-dev/meta/session/{coord_type}')
    log.info(f"coord_type: {coord_type}, etcd session value: {v}")
    activated_pod_ip = json.loads(v[0])["Address"].split(":")[0]
    label_selector = f'app.kubernetes.io/instance={release_name}, component={coord_type}'
    items = get_pod_list(namespace, label_selector=label_selector)
    all_pod_list = []
    for item in items:
        pod_name = item.metadata.name
        all_pod_list.append(pod_name)
    activate_pod_list = []
    standby_pod_list = []
    for item in items:
        pod_name = item.metadata.name
        ip = item.status.pod_ip
        if ip == activated_pod_ip:
            activate_pod_list.append(pod_name)
    standby_pod_list = list(set(all_pod_list) - set(activate_pod_list))
    return activate_pod_list, standby_pod_list


def reset_healthy_checker_after_standby_activated(namespace, release_name, coord_type, health_checkers, timeout=360):
    activate_pod_list_before, standby_pod_list_before = find_activate_standby_coord_pod(namespace, release_name, coord_type)
    log.info(f"check standby switch: activate_pod_list_before {activate_pod_list_before}, "
             f"standby_pod_list_before {standby_pod_list_before}")
    standby_activated = False
    start_time = time.time()
    end_time = time.time()
    while not standby_activated and end_time - start_time < timeout:
        try:
            activate_pod_list_after, standby_pod_list_after = find_activate_standby_coord_pod(namespace, release_name, coord_type)
            if activate_pod_list_after[0] in standby_pod_list_before:
                standby_activated = True
                log.info(f"Standby {coord_type} pod {activate_pod_list_after[0]} activated")
                log.info(f"check standby switch: activate_pod_list_after {activate_pod_list_after}, "
                         f"standby_pod_list_after {standby_pod_list_after}")
                break
        except Exception as e:
            log.error(f"Exception when check standby switch: {e}")
        time.sleep(10)
        end_time = time.time()
    if standby_activated:
        time.sleep(30)
        cc.reset_counting(health_checkers)
        for k, v in health_checkers.items():
            log.info("reset health checkers")
            v.check_result()
    else:
        log.info(f"Standby {coord_type} pod does not switch standby mode")


if __name__ == '__main__':
    # for coord in ["indexcoord"]:
    for coord in ["rootcoord", "datacoord", "indexcoord", "querycoord"]:
        activate_pod_list, standby_pod_list = find_activate_standby_coord_pod("chaos-testing", "rootcoord-standby-test", coord)
        print(f"activate pod {activate_pod_list}, standby pod {standby_pod_list}")
    # label = "app.kubernetes.io/name=milvus, component=querynode"
    # instance_name = get_milvus_instance_name("chaos-testing", "10.96.250.111")
    # res = get_pod_list("chaos-testing", label_selector=label)
    # m = get_pod_ip_name_pairs("chaos-testing", label_selector=label)
    # export_pod_logs(namespace='chaos-testing', label_selector=label)
