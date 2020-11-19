import os
import pdb
import time
import logging
from yaml import full_load, dump
import utils

logger = logging.getLogger("milvus_benchmark.utils")
REGISTRY_URL = "registry.zilliz.com/milvus/engine"


def retry(times):
    """
    This decorator prints the execution time for the decorated function.
    """
    def wrapper(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    logger.info(attempt)
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.info(str(e))
                    time.sleep(3)
                    attempt += 1
            return func(*args, **kwargs)
        return newfn
    return wrapper


@utils.retry(3)
def get_host_cpus(hostname):
    from kubernetes import client, config
    config.load_kube_config()
    client.rest.logger.setLevel(logging.WARNING)
    v1 = client.CoreV1Api()
    cpus = v1.read_node(hostname).status.allocatable.get("cpu")
    return cpus


# update values.yaml
def update_values(file_path, deploy_mode, hostname, milvus_config, server_config=None):
    if not os.path.isfile(file_path):
        raise Exception('File: %s not found' % file_path)
    # 　bak values.yaml
    file_name = os.path.basename(file_path)
    bak_file_name = file_name + ".bak"
    file_parent_path = os.path.dirname(file_path)
    bak_file_path = file_parent_path + '/' + bak_file_name
    if os.path.exists(bak_file_path):
        os.system("cp %s %s" % (bak_file_path, file_path))
    else:
        os.system("cp %s %s" % (file_path, bak_file_path))
    with open(file_path) as f:
        values_dict = full_load(f)
        f.close()

    for k, v in milvus_config.items():
        if k.find("primary_path") != -1:
            suffix_path = milvus_config["suffix_path"] if "suffix_path" in milvus_config else None
            path_value = v
            if suffix_path:
                path_value = v + "_" + str(int(time.time()))
            values_dict["primaryPath"] = path_value
            values_dict['wal']['path'] = path_value + "/wal"
            values_dict['logs']['path'] = path_value + "/logs"
        # elif k.find("use_blas_threshold") != -1:
        #     values_dict['useBLASThreshold'] = int(v)
        elif k.find("gpu_search_threshold") != -1:
            values_dict['gpu']['gpuSearchThreshold'] = int(v)
        elif k.find("cpu_cache_capacity") != -1:
            values_dict['cache']['cacheSize'] = v
        # elif k.find("cache_insert_data") != -1:
        #     values_dict['cache']['cacheInsertData'] = v
        elif k.find("insert_buffer_size") != -1:
            values_dict['cache']['insertBufferSize'] = v
        elif k.find("gpu_resource_config.enable") != -1:
            values_dict['gpu']['enabled'] = v
        elif k.find("gpu_resource_config.cache_capacity") != -1:
            values_dict['gpu']['cacheSize'] = v
        elif k.find("build_index_resources") != -1:
            values_dict['gpu']['buildIndexDevices'] = v
        elif k.find("search_resources") != -1:
            values_dict['gpu']['searchDevices'] = v
        # wal
        elif k.find("auto_flush_interval") != -1:
            values_dict['storage']['autoFlushInterval'] = v
        elif k.find("wal_enable") != -1:
            values_dict['wal']['enabled'] = v

    # if values_dict['nodeSelector']:
    #     logger.warning("nodeSelector has been set: %s" % str(values_dict['engine']['nodeSelector']))
    #     return
    values_dict["wal"]["recoveryErrorIgnore"] = True
    # enable monitor
    values_dict["metrics"]["enabled"] = True
    values_dict["metrics"]["address"] = "192.168.1.237"
    values_dict["metrics"]["port"] = 9091
    # Using sqlite for single mode
    if deploy_mode == "single":
        values_dict["mysql"]["enabled"] = False

    # update values.yaml with the given host
    if hostname:
        values_dict['nodeSelector'] = {'kubernetes.io/hostname': hostname}
        cpus = server_config["cpus"]

        # set limit/request cpus in resources
        values_dict["image"]['resources'] = {
            "limits": {
                # "cpu": str(int(cpus)) + ".0"
                "cpu": str(int(cpus)) + ".0"
            },
            "requests": {
                "cpu": str(int(cpus) - 1) + ".0"
            }
        }
        values_dict['tolerations'] = [{
            "key": "worker",
            "operator": "Equal",
            "value": "performance",
            "effect": "NoSchedule"
        }]
    # add extra volumes
    values_dict['extraVolumes'] = [{
        'name': 'test',
        'flexVolume': {
            'driver': "fstab/cifs",
            'fsType': "cifs",
            'secretRef': {
                'name': "cifs-test-secret"
            },
            'options': {
                'networkPath': "//192.168.1.126/test",
                'mountOptions': "vers=1.0"
            }
        }
    }]
    values_dict['extraVolumeMounts'] = [{
        'name': 'test',
        'mountPath': '/test'
    }]

    # add extra volumes for mysql
    # values_dict['mysql']['persistence']['enabled'] = True
    # values_dict['mysql']['configurationFilesPath'] = "/etc/mysql/mysql.conf.d/"
    # values_dict['mysql']['imageTag'] = '5.6'
    # values_dict['mysql']['securityContext'] = {
    #         'enabled': True}
    # mysql_db_path = "/test"
    if deploy_mode == "shards":
        # mount_path = values_dict["primaryPath"]+'/data'
        # long_str = '- name: test-mysql\n  flexVolume:\n    driver: fstab/cifs\n    fsType: cifs\n    secretRef:\n      name: cifs-test-secret\n    options:\n      networkPath: //192.168.1.126/test\n      mountOptions: vers=1.0'
        # values_dict['mysql']['extraVolumes'] = literal_str(long_str)
        # long_str_2 = "- name: test-mysql\n  mountPath: %s" % mysql_db_path
        # values_dict['mysql']['extraVolumeMounts'] = literal_str(long_str_2)
        # mysql_cnf_str = '[mysqld]\npid-file=%s/mysql.pid\ndatadir=%s' % (mount_path, mount_path)
        # values_dict['mysql']['configurationFiles'] = {}
        # values_dict['mysql']['configurationFiles']['mysqld.cnf'] = literal_str(mysql_cnf_str)

        values_dict['mysql']['enabled'] = False
        values_dict['externalMysql']['enabled'] = True
        values_dict['externalMysql']["ip"] = "192.168.1.197"
        values_dict['externalMysql']["port"] = 3306
        values_dict['externalMysql']["user"] = "root"
        values_dict['externalMysql']["password"] = "Fantast1c"
        values_dict['externalMysql']["database"] = "db"

    # logger.debug(values_dict)
    #  print(dump(values_dict))
    with open(file_path, 'w') as f:
        dump(values_dict, f, default_flow_style=False)
    f.close()
    # DEBUG
    with open(file_path) as f:
        for line in f.readlines():
            line = line.strip("\n")


# deploy server
def helm_install_server(helm_path, deploy_mode, image_tag, image_type, name, namespace):
    timeout = 300
    logger.debug("Server deploy mode: %s" % deploy_mode)
    host = "%s.%s.svc.cluster.local" % (name, namespace)
    if deploy_mode == "single":
        install_cmd = "helm install --wait --timeout %ds \
                --set image.repository=%s \
                --set image.tag=%s \
                --set image.pullPolicy=Always \
                --set service.type=ClusterIP \
                -f ci/filebeat/values.yaml \
                --namespace %s \
                %s ." % (timeout, REGISTRY_URL, image_tag, namespace, name)
    elif deploy_mode == "shards":
        install_cmd = "helm install --wait --timeout %ds \
                --set cluster.enabled=true \
                --set persistence.enabled=true \
                --set mishards.image.tag=test \
                --set mishards.image.pullPolicy=Always \
                --set image.repository=%s \
                --set image.tag=%s \
                --set image.pullPolicy=Always \
                --set service.type=ClusterIP \
                -f ci/filebeat/values.yaml \
                --namespace %s \
                %s ." % (timeout, REGISTRY_URL, image_tag, namespace, name)
    logger.debug(install_cmd)
    logger.debug(host)
    if os.system("cd %s && %s" % (helm_path, install_cmd)):
        logger.error("Helm install failed: %s" % name)
        return None
    time.sleep(5)
    # config.load_kube_config()
    # v1 = client.CoreV1Api()
    # pod_name = None
    # pod_id = None
    # pods = v1.list_namespaced_pod(namespace)
    # for i in pods.items:
    #     if i.metadata.name.find(name) != -1:
    #         pod_name = i.metadata.name
    #         pod_ip = i.status.pod_ip
    # logger.debug(pod_name)
    # logger.debug(pod_ip)
    # return pod_name, pod_ip
    return host


# delete server
def helm_del_server(name, namespace):
    # logger.debug("Sleep 600s before uninstall server")
    # time.sleep(600)
    del_cmd = "helm uninstall -n milvus %s" % name
    logger.debug(del_cmd)
    if os.system(del_cmd):
        logger.error("Helm delete name:%s failed" % name)
        return False
    return True


def restart_server(helm_release_name, namespace):
    res = True
    timeout = 120000
    # service_name = "%s.%s.svc.cluster.local" % (helm_release_name, namespace)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    pod_name = None
    # config_map_names = v1.list_namespaced_config_map(namespace, pretty='true')
    # body = {"replicas": 0}
    pods = v1.list_namespaced_pod(namespace)
    for i in pods.items:
        if i.metadata.name.find(helm_release_name) != -1 and i.metadata.name.find("mysql") == -1:
            pod_name = i.metadata.name
            break
            # v1.patch_namespaced_config_map(config_map_name, namespace, body, pretty='true')
    # status_res = v1.read_namespaced_service_status(helm_release_name, namespace, pretty='true')
    logger.debug("Pod name: %s" % pod_name)
    if pod_name is not None:
        try:
            v1.delete_namespaced_pod(pod_name, namespace)
        except Exception as e:
            logging.error(str(e))
            logging.error("Exception when calling CoreV1Api->delete_namespaced_pod")
            res = False
            return res
        logging.error("Sleep 10s after pod deleted")
        time.sleep(10)
        # check if restart successfully
        pods = v1.list_namespaced_pod(namespace)
        for i in pods.items:
            pod_name_tmp = i.metadata.name
            logging.error(pod_name_tmp)
            if pod_name_tmp == pod_name:
                continue
            elif pod_name_tmp.find(helm_release_name) == -1 or pod_name_tmp.find("mysql") != -1:
                continue
            else:
                status_res = v1.read_namespaced_pod_status(pod_name_tmp, namespace, pretty='true')
                logging.error(status_res.status.phase)
                start_time = time.time()
                ready_break = False
                while time.time() - start_time <= timeout:
                    logging.error(time.time())
                    status_res = v1.read_namespaced_pod_status(pod_name_tmp, namespace, pretty='true')
                    if status_res.status.phase == "Running":
                        logging.error("Already running")
                        ready_break = True
                        break
                    else:
                        time.sleep(5)
                if time.time() - start_time > timeout:
                    logging.error("Restart pod: %s timeout" % pod_name_tmp)
                    res = False
                    return res
                if ready_break:
                    break
    else:
        raise Exception("Pod: %s not found" % pod_name)
    follow = True
    pretty = True
    previous = True  # bool | Return previous terminated container logs. Defaults to false. (optional)
    since_seconds = 56  # int | A relative time in seconds before the current time from which to show logs. If this value precedes the time a pod was started, only logs since the pod start will be returned. If this value is in the future, no logs will be returned. Only one of sinceSeconds or sinceTime may be specified. (optional)
    timestamps = True  # bool | If true, add an RFC3339 or RFC3339Nano timestamp at the beginning of every line of log output. Defaults to false. (optional)
    container = "milvus"
    # start_time = time.time()
    # while time.time() - start_time <= timeout:
    #     try:
    #         api_response = v1.read_namespaced_pod_log(pod_name_tmp, namespace, container=container, follow=follow,
    #                                                 pretty=pretty, previous=previous, since_seconds=since_seconds,
    #                                                 timestamps=timestamps)
    #         logging.error(api_response)
    #         return res
    #     except Exception as e:
    #         logging.error("Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)
    #         # waiting for server start
    #         time.sleep(2)
    #         # res = False
    #         # return res
    # if time.time() - start_time > timeout:
    #     logging.error("Restart pod: %s timeout" % pod_name_tmp)
    #     res = False
    return res


if __name__ == '__main__':
    update_values("", "shards", None, None)
