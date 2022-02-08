import os.path
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from utils.util_log import test_log as log


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
    config.load_kube_config()
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
                        log.info(f"{c.name} status is {c.ready}")
                        if c.ready is False:
                            all_pos_ready_flag = False
                            break
            if not all_pos_ready_flag:
                log.info("all pods are not ready, please wait")
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
    config.load_kube_config()
    api_instance = client.CoreV1Api()
    try:
        api_response = api_instance.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        return api_response.items
    except ApiException as e:
        log.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        raise Exception(str(e))


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
    config.load_kube_config()
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


if __name__ == '__main__':
    label = "app.kubernetes.io/instance=test-proxy-pod-failure, component=proxy"
    res = get_pod_list("chaos-testing", label_selector=label)
    export_pod_logs(namespace='chaos-testing', label_selector=label)
