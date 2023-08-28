import os
import pdb
import time
import logging
import hashlib
import traceback
from yaml import full_load, dump
from milvus_benchmark import utils
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.env.helm_utils")
BOOKKEEPER_PULSAR_MEM = '\"-Xms512m -Xmx1024m -XX:MaxDirectMemorySize=1024m -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.linkCapacity=1024 -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB -XX:+ExitOnOutOfMemoryError -XX:+PerfDisableSharedMem -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -verbosegc -XX:G1LogLevel=finest\"'
BROKER_PULSAR_MEM = '\"-Xms512m -Xmx1024m -XX:MaxDirectMemorySize=1024m -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.linkCapacity=1024 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB -XX:+ExitOnOutOfMemoryError -XX:+PerfDisableSharedMem\"'


def get_host_cpus(hostname):
    from kubernetes import client, config
    config.load_kube_config()
    client.rest.logger.setLevel(logging.WARNING)
    try:
        v1 = client.CoreV1Api()
        cpus = v1.read_node(hostname).status.allocatable.get("cpu")
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(str(e))
        cpus = 0
    finally:
        return cpus


def update_server_config(server_name, server_tag, server_config):
    cpus = config.DEFAULT_CPUS
    if server_name:
        try:
            cpus = get_host_cpus(server_name)
            if not cpus:
                cpus = config.DEFAULT_CPUS
        except Exception as e:
            logger.error("Get cpus on host: {} failed".format(server_name))
            logger.error(str(e))
        if server_config:
            if "cpus" in server_config.keys():
                cpus = server_config["cpus"]
        # self.hardware = Hardware(name=self.hostname, cpus=cpus)
    if server_tag:
        cpus = int(server_tag.split("c")[0])
    kv = {"cpus": cpus}
    logger.debug(kv)
    if server_config:
        server_config.update(kv)
    else:
        server_config = kv
    return server_config


"""
description: update values.yaml
return: no return
"""


def update_values(file_path, deploy_mode, hostname, server_tag, milvus_config, server_config=None):
    # bak values.yaml
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
    cluster = False
    if deploy_mode == "cluster":
        cluster = True

    # TODO: disable change config
    # cluster = False
    # if "cluster" in milvus_config and milvus_config["cluster"]:
    #     cluster = True
    # for k, v in milvus_config.items():
    #     if k.find("primary_path") != -1:
    #         suffix_path = milvus_config["suffix_path"] if "suffix_path" in milvus_config else None
    #         path_value = v
    #         if suffix_path:
    #             path_value = v + "_" + str(int(time.time()))
    #         values_dict["primaryPath"] = path_value
    #         values_dict['wal']['path'] = path_value + "/wal"
    #         values_dict['logs']['path'] = path_value + "/logs"
    #     # elif k.find("use_blas_threshold") != -1:
    #     #     values_dict['useBLASThreshold'] = int(v)
    #     elif k.find("gpu_search_threshold") != -1:
    #         values_dict['gpu']['gpuSearchThreshold'] = int(v)
    #         if cluster:
    #             values_dict['readonly']['gpu']['gpuSearchThreshold'] = int(v)
    #     elif k.find("cpu_cache_capacity") != -1:
    #         values_dict['cache']['cacheSize'] = v
    #         if cluster:
    #             values_dict['readonly']['cache']['cacheSize'] = v
    #     # elif k.find("cache_insert_data") != -1:
    #     #     values_dict['cache']['cacheInsertData'] = v
    #     elif k.find("insert_buffer_size") != -1:
    #         values_dict['cache']['insertBufferSize'] = v
    #         if cluster:
    #             values_dict['readonly']['cache']['insertBufferSize'] = v
    #     elif k.find("gpu_resource_config.enable") != -1:
    #         values_dict['gpu']['enabled'] = v
    #         if cluster:
    #             values_dict['readonly']['gpu']['enabled'] = v
    #     elif k.find("gpu_resource_config.cache_capacity") != -1:
    #         values_dict['gpu']['cacheSize'] = v
    #         if cluster:
    #             values_dict['readonly']['gpu']['cacheSize'] = v
    #     elif k.find("build_index_resources") != -1:
    #         values_dict['gpu']['buildIndexDevices'] = v
    #         if cluster:
    #             values_dict['readonly']['gpu']['buildIndexDevices'] = v
    #     elif k.find("search_resources") != -1:
    #         values_dict['gpu']['searchDevices'] = v
    #         if cluster:
    #             values_dict['readonly']['gpu']['searchDevices'] = v
    #     # wal
    #     elif k.find("auto_flush_interval") != -1:
    #         values_dict['storage']['autoFlushInterval'] = v
    #         if cluster:
    #             values_dict['readonly']['storage']['autoFlushInterval'] = v
    #     elif k.find("wal_enable") != -1:
    #         values_dict['wal']['enabled'] = v

    # # if values_dict['nodeSelector']:
    # #     logger.warning("nodeSelector has been set: %s" % str(values_dict['engine']['nodeSelector']))
    # #     return
    # values_dict["wal"]["recoveryErrorIgnore"] = True
    # # enable monitor
    # values_dict["metrics"]["enabled"] = True
    # values_dict["metrics"]["address"] = "192.168.1.237"
    # values_dict["metrics"]["port"] = 9091
    # # only test avx2 
    # values_dict["extraConfiguration"].update({"engine": {"simd_type": "avx2"}})
    # # stat_optimizer_enable
    # values_dict["extraConfiguration"]["engine"].update({"stat_optimizer_enable": False})

    # # enable read-write mode
    # if cluster:
    #     values_dict["cluster"]["enabled"] = True
    #     # update readonly log path
    #     values_dict["readonly"]['logs']['path'] = values_dict['logs']['path'] + "/readonly"
    #     if "readonly" in milvus_config:
    #         if "replicas" in milvus_config["readonly"]:
    #             values_dict["readonly"]["replicas"] = milvus_config["readonly"]["replicas"]


    # # update values.yaml with the given host
    node_config = None
    perf_tolerations = [{
        "key": "worker",
        "operator": "Equal",
        "value": "performance",
        "effect": "NoSchedule"
    }]
    if hostname:
        node_config = {'kubernetes.io/hostname': hostname}
    elif server_tag:
        # server tag
        node_config = {'instance-type': server_tag}
    cpus = server_config["cpus"]
    logger.debug(hostname)
    if cluster is False:
        if node_config:
            values_dict['standalone']['nodeSelector'] = node_config
            values_dict['minio']['nodeSelector'] = node_config
            values_dict['etcd']['nodeSelector'] = node_config
            # TODO: disable
            # set limit/request cpus in resources
            values_dict['standalone']['resources'] = {
                "limits": {
                    # "cpu": str(int(cpus)) + ".0"
                    "cpu": str(int(cpus)) + ".0"
                },
                "requests": {
                    "cpu": str(int(cpus) // 2 + 1) + ".0"
                    # "cpu": "4.0"
                }
            }
            logger.debug("Add tolerations into standalone server")
            values_dict['standalone']['tolerations'] = perf_tolerations
            values_dict['minio']['tolerations'] = perf_tolerations
            values_dict['etcd']['tolerations'] = perf_tolerations
    else:
        # values_dict['pulsar']["broker"]["configData"].update({"maxMessageSize": "52428800", "PULSAR_MEM": BOOKKEEPER_PULSAR_MEM})
        # values_dict['pulsar']["bookkeeper"]["configData"].update({"nettyMaxFrameSizeBytes": "52428800", "PULSAR_MEM": BROKER_PULSAR_MEM})
        values_dict['proxynode']['nodeSelector'] = node_config
        values_dict['querynode']['nodeSelector'] = node_config
        values_dict['indexnode']['nodeSelector'] = node_config
        values_dict['datanode']['nodeSelector'] = node_config
        values_dict['minio']['nodeSelector'] = node_config

        # values_dict['pulsar']["enabled"] = True
        # values_dict['pulsar']['autoRecovery']['nodeSelector'] = node_config
        # values_dict['pulsar']['proxy']['nodeSelector'] = node_config
        # values_dict['pulsar']['broker']['nodeSelector'] = node_config
        # values_dict['pulsar']['bookkeeper']['nodeSelector'] = node_config
        # values_dict['pulsar']['zookeeper']['nodeSelector'] = node_config
        values_dict['pulsarStandalone']['nodeSelector'] = node_config
        if hostname:
            logger.debug("Add tolerations into cluster server")
            values_dict['proxynode']['tolerations'] = perf_tolerations
            values_dict['querynode']['tolerations'] = perf_tolerations
            values_dict['indexnode']['tolerations'] = perf_tolerations
            values_dict['datanode']['tolerations'] = perf_tolerations
            values_dict['etcd']['tolerations'] = perf_tolerations
            values_dict['minio']['tolerations'] = perf_tolerations
            values_dict['pulsarStandalone']['tolerations'] = perf_tolerations
            # values_dict['pulsar']['autoRecovery']['tolerations'] = perf_tolerations
            # values_dict['pulsar']['proxy']['tolerations'] = perf_tolerations
            # values_dict['pulsar']['broker']['tolerations'] = perf_tolerations
            # values_dict['pulsar']['bookkeeper']['tolerations'] = perf_tolerations
            # values_dict['pulsar']['zookeeper']['tolerations'] = perf_tolerations

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
                'networkPath': config.IDC_NAS_URL,
                'mountOptions': "vers=1.0"
            }
        }
    }]
    values_dict['extraVolumeMounts'] = [{
        'name': 'test',
        'mountPath': '/test'
    }]

    with open(file_path, 'w') as f:
        dump(values_dict, f, default_flow_style=False)
    f.close()
    # DEBUG
    with open(file_path) as f:
        for line in f.readlines():
            line = line.strip("\n")
            logger.debug(line)


# deploy server
def helm_install_server(helm_path, deploy_mode, image_tag, image_type, name, namespace):
    logger.debug("Server deploy mode: %s" % deploy_mode)
    host = "%s-milvus-ha.%s.svc.cluster.local" % (name, namespace)
    # TODO: update etcd config
    etcd_config_map_cmd = "kubectl create configmap -n %s %s --from-literal=ETCD_QUOTA_BACKEND_BYTES=8589934592 --from-literal=ETCD_SNAPSHOT_COUNT=5000 --from-literal=ETCD_AUTO_COMPACTION_MODE=revision --from-literal=ETCD_AUTO_COMPACTION_RETENTION=1" % (
        namespace, name)
    if os.system(etcd_config_map_cmd):
        raise Exception("Create configmap: {} failed".format(name))
    logger.debug("Create configmap: {} successfully".format(name))
    log_path = config.LOG_PATH + "install.log"
    install_cmd = "helm install \
            --set standalone.service.type=ClusterIP \
            --set image.all.repository=%s \
            --set image.all.tag=%s \
            --set minio.persistence.enabled=false \
            --set etcd.persistence.enabled=false \
            --set etcd.envVarsConfigMap=%s \
            --namespace %s \
            %s . >>%s >&1" % (config.REGISTRY_URL, image_tag, name, namespace, name, log_path)
    # --set image.all.pullPolicy=Always \
    if deploy_mode == "cluster":
        install_cmd = "helm install \
                --set standalone.enabled=false \
                --set image.all.repository=%s \
                --set image.all.tag=%s \
                --set minio.persistence.enabled=false \
                --set etcd.persistence.enabled=false \
                --set etcd.envVarsConfigMap=%s \
                --namespace %s \
                %s . >>%s >&1" % (config.REGISTRY_URL, image_tag, name, namespace, name, log_path)
        # --set image.all.pullPolicy=Always \
    elif deploy_mode != "single":
        raise Exception("Deploy mode: {} not support".format(deploy_mode))
    logger.debug(install_cmd)
    logger.debug(host)
    if os.system("cd %s && %s" % (helm_path, install_cmd)):
        logger.error("Helm install failed: %s" % name)
        return None
    logger.debug("Wait for 60s ..")
    time.sleep(60)
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
@utils.retry(3)
def helm_del_server(name, namespace):
    # logger.debug("Sleep 600s before uninstall server")
    # time.sleep(600)
    delete_etcd_config_map_cmd = "kubectl delete configmap -n %s %s" % (namespace, name)
    logger.info(delete_etcd_config_map_cmd)
    if os.system(delete_etcd_config_map_cmd):
        logger.error("Delete configmap %s:%s failed" % (namespace, name))
    del_cmd = "helm uninstall -n milvus %s" % name
    logger.info(del_cmd)
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
        if i.metadata.name.find(helm_release_name) != -1:
            pod_name = i.metadata.name
            break
            # v1.patch_namespaced_config_map(config_map_name, namespace, body, pretty='true')
    # status_res = v1.read_namespaced_service_status(helm_release_name, namespace, pretty='true')
    logger.debug("Pod name: %s" % pod_name)
    if pod_name is not None:
        try:
            v1.delete_namespaced_pod(pod_name, namespace)
        except Exception as e:
            logger.error(str(e))
            logger.error("Exception when calling CoreV1Api->delete_namespaced_pod")
            res = False
            return res
        logger.error("Sleep 10s after pod deleted")
        time.sleep(10)
        # check if restart successfully
        pods = v1.list_namespaced_pod(namespace)
        for i in pods.items:
            pod_name_tmp = i.metadata.name
            logger.error(pod_name_tmp)
            if pod_name_tmp == pod_name:
                continue
            elif pod_name_tmp.find(helm_release_name) == -1:
                continue
            else:
                status_res = v1.read_namespaced_pod_status(pod_name_tmp, namespace, pretty='true')
                logger.error(status_res.status.phase)
                start_time = time.time()
                ready_break = False
                while time.time() - start_time <= timeout:
                    logger.error(time.time())
                    status_res = v1.read_namespaced_pod_status(pod_name_tmp, namespace, pretty='true')
                    if status_res.status.phase == "Running":
                        logger.error("Already running")
                        ready_break = True
                        break
                    else:
                        time.sleep(5)
                if time.time() - start_time > timeout:
                    logger.error("Restart pod: %s timeout" % pod_name_tmp)
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


def get_pod_status(helm_release_name, namespace):
    from kubernetes import client, config
    config.load_kube_config()
    v1 = client.CoreV1Api()
    pod_status = []
    label_selector = 'app.kubernetes.io/instance={}'.format(helm_release_name)
    # pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)
    pods = v1.list_namespaced_pod(namespace)
    for i in pods.items:
        if i.metadata.name.find(helm_release_name) != -1:
            pod_name = i.metadata.name
            result = v1.read_namespaced_pod_status(pod_name, namespace)
            pod_status.append({"pod": pod_name, "status": result.status.phase})
    # print(pod_status)
    return pod_status


def running_status(helm_release_name, namespace):
    pod_status = get_pod_status(helm_release_name, namespace)
    for pod in pod_status:
        if pod["status"] != "Running":
            return False
    return True


if __name__ == '__main__':
    def ff():
        namespace = 'milvus'
        helm_release_name = 'zong-standalone'
        # st = get_pod_status(helm_release_name, namespace)
        status = get_pod_status(helm_release_name, namespace)
        print(status)
        for s in status:
            if s["status"] != "Runningk":
                return False
        return True


    def fff():
        print(time.time())


    while not ff():
        print("retry")
    else:
        print("gogog")
    print("hhhh")
