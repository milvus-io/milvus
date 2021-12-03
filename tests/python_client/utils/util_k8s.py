import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from utils.util_log import test_log as log


def wait_pods_ready(namespace, label_selector, timeout=360):
    """wait pods with label selector all ready"""
    config.load_kube_config()
    api_instance = client.CoreV1Api()
    try:
        all_pos_ready_flag = False
        time_cnt = 0
        while not all_pos_ready_flag and time_cnt < timeout:
            api_response = api_instance.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
            all_pos_ready_flag = True
            for item in api_response.items:
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
            log.info("timeout for waiting all pods in namespace {namespace} with label {label_selector} ready")
    except ApiException as e:
        log.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        raise Exception(str(e))

    return all_pos_ready_flag


if __name__ == '__main__':
    wait_pods_ready("chaos-testing", "app.kubernetes.io/instance=scale-query")
