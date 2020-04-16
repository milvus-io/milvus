import os
import sys
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

import re
import logging
import time
import copy
import threading
import queue
import enum
from functools import partial
from collections import defaultdict
from kubernetes import client, config as kconfig, watch
from mishards.topology import StatusType

logger = logging.getLogger(__name__)

INCLUSTER_NAMESPACE_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'


class EventType(enum.Enum):
    PodHeartBeat = 1
    Watch = 2


class K8SMixin:
    def __init__(self, namespace, in_cluster=False, **kwargs):
        self.namespace = namespace
        self.in_cluster = in_cluster
        self.kwargs = kwargs
        self.v1 = kwargs.get('v1', None)
        if not self.namespace:
            self.namespace = open(INCLUSTER_NAMESPACE_PATH).read()

        if not self.v1:
            kconfig.load_incluster_config(
            ) if self.in_cluster else kconfig.load_kube_config()
            self.v1 = client.CoreV1Api()


class K8SHeartbeatHandler(threading.Thread, K8SMixin):
    name = 'kubernetes'

    def __init__(self,
                 message_queue,
                 namespace,
                 label_selector,
                 in_cluster=False,
                 **kwargs):
        K8SMixin.__init__(self,
                          namespace=namespace,
                          in_cluster=in_cluster,
                          **kwargs)
        threading.Thread.__init__(self)
        self.queue = message_queue
        self.terminate = False
        self.label_selector = label_selector
        self.poll_interval = kwargs.get('poll_interval', 5)

    def run(self):
        while not self.terminate:
            try:
                pods = self.v1.list_namespaced_pod(
                    namespace=self.namespace,
                    label_selector=self.label_selector)
                event_message = {'eType': EventType.PodHeartBeat, 'events': []}
                for item in pods.items:
                    pod = self.v1.read_namespaced_pod(name=item.metadata.name,
                                                      namespace=self.namespace)
                    name = pod.metadata.name
                    ip = pod.status.pod_ip
                    phase = pod.status.phase
                    reason = pod.status.reason
                    message = pod.status.message
                    ready = True if phase == 'Running' else False

                    pod_event = dict(pod=name,
                                     ip=ip,
                                     ready=ready,
                                     reason=reason,
                                     message=message)

                    event_message['events'].append(pod_event)

                self.queue.put(event_message)

            except Exception as exc:
                logger.error(exc)

            time.sleep(self.poll_interval)

    def stop(self):
        self.terminate = True


class K8SEventListener(threading.Thread, K8SMixin):
    def __init__(self, message_queue, namespace, in_cluster=False, **kwargs):
        K8SMixin.__init__(self,
                          namespace=namespace,
                          in_cluster=in_cluster,
                          **kwargs)
        threading.Thread.__init__(self)
        self.queue = message_queue
        self.terminate = False
        self.at_start_up = True
        self._stop_event = threading.Event()

    def stop(self):
        self.terminate = True
        self._stop_event.set()

    def run(self):
        resource_version = ''
        w = watch.Watch()
        for event in w.stream(self.v1.list_namespaced_event,
                              namespace=self.namespace,
                              field_selector='involvedObject.kind=Pod'):
            if self.terminate:
                break

            resource_version = int(event['object'].metadata.resource_version)

            info = dict(
                eType=EventType.Watch,
                pod=event['object'].involved_object.name,
                reason=event['object'].reason,
                message=event['object'].message,
                start_up=self.at_start_up,
            )
            self.at_start_up = False
            # logger.info('Received event: {}'.format(info))
            self.queue.put(info)


class EventHandler(threading.Thread):
    PENDING_THRESHOLD = 3
    def __init__(self, mgr, message_queue, namespace, pod_patt, **kwargs):
        threading.Thread.__init__(self)
        self.mgr = mgr
        self.queue = message_queue
        self.kwargs = kwargs
        self.terminate = False
        self.pod_patt = re.compile(pod_patt)
        self.namespace = namespace
        self.pending_add = defaultdict(int)
        self.pending_delete = defaultdict(int)

    def record_pending_add(self, pod, true_cb=None):
        self.pending_add[pod] += 1
        self.pending_delete.pop(pod, None)
        if self.pending_add[pod] >= self.PENDING_THRESHOLD:
            true_cb and true_cb()
            return True
        return False

    def record_pending_delete(self, pod, true_cb=None):
        self.pending_delete[pod] += 1
        self.pending_add.pop(pod, None)
        if self.pending_delete[pod] >= 1:
            true_cb and true_cb()
            return True

        return False

    def stop(self):
        self.terminate = True

    def on_drop(self, event, **kwargs):
        pass

    def on_pod_started(self, event, **kwargs):
        try_cnt = 3
        pod = None
        while try_cnt > 0:
            try_cnt -= 1
            try:
                pod = self.mgr.v1.read_namespaced_pod(name=event['pod'],
                                                      namespace=self.namespace)
                if not pod.status.pod_ip:
                    time.sleep(0.5)
                    continue
                break
            except client.rest.ApiException as exc:
                time.sleep(0.5)

        if try_cnt <= 0 and not pod:
            if not event['start_up']:
                logger.warning('Pod {} is started but cannot read pod'.format(
                    event['pod']))
            return
        elif try_cnt <= 0 and not pod.status.pod_ip:
            logger.warning('NoPodIPFoundError')
            return

        self.record_pending_add(pod.metadata.name,
                true_cb=partial(self.mgr.add_pod, pod.metadata.name, pod.status.pod_ip))

    def on_pod_killing(self, event, **kwargs):
        self.record_pending_delete(event['pod'],
                true_cb=partial(self.mgr.delete_pod, event['pod']))

    def on_pod_heartbeat(self, event, **kwargs):
        names = set(copy.deepcopy(list(self.mgr.readonly_topo.group_names)))

        pods_with_event = set()
        for each_event in event['events']:
            pods_with_event.add(each_event['pod'])
            if each_event['ready']:
                self.record_pending_add(each_event['pod'],
                    true_cb=partial(self.mgr.add_pod, each_event['pod'], each_event['ip']))
            else:
                self.record_pending_delete(each_event['pod'],
                    true_cb=partial(self.mgr.delete_pod, each_event['pod']))

        pods_no_event = names - pods_with_event
        for name in pods_no_event:
            self.record_pending_delete(name,
                    true_cb=partial(self.mgr.delete_pod, name))

        latest = self.mgr.readonly_topo.group_names
        deleted = names - latest
        added = latest - names
        if deleted:
            logger.info('Deleted Pods: {}'.format(list(deleted)))
        if added:
            logger.info('Added Pods: {}'.format(list(added)))

        logger.debug('All Pods: {}'.format(list(latest)))

    def handle_event(self, event):
        if event['eType'] == EventType.PodHeartBeat:
            return self.on_pod_heartbeat(event)

        if not event or (event['reason'] not in ('Started', 'Killing')):
            return self.on_drop(event)

        if not re.match(self.pod_patt, event['pod']):
            return self.on_drop(event)

        logger.info('Handling event: {}'.format(event))

        if event['reason'] == 'Started':
            return self.on_pod_started(event)

        return self.on_pod_killing(event)

    def run(self):
        while not self.terminate:
            try:
                event = self.queue.get(timeout=1)
                self.handle_event(event)
            except queue.Empty:
                continue


class KubernetesProviderSettings:
    def __init__(self, namespace, pod_patt, label_selector, in_cluster,
                 poll_interval, port=None, **kwargs):
        self.namespace = namespace
        self.pod_patt = pod_patt
        self.label_selector = label_selector
        self.in_cluster = in_cluster
        self.poll_interval = poll_interval
        self.port = int(port) if port else 19530


class KubernetesProvider(object):
    name = 'kubernetes'

    def __init__(self, config, readonly_topo, **kwargs):
        self.namespace = config.DISCOVERY_KUBERNETES_NAMESPACE
        self.pod_patt = config.DISCOVERY_KUBERNETES_POD_PATT
        self.label_selector = config.DISCOVERY_KUBERNETES_LABEL_SELECTOR
        self.in_cluster = config.DISCOVERY_KUBERNETES_IN_CLUSTER.lower()
        self.in_cluster = self.in_cluster == 'true'
        self.poll_interval = config.DISCOVERY_KUBERNETES_POLL_INTERVAL
        self.poll_interval = int(self.poll_interval) if self.poll_interval else 5
        self.port = config.DISCOVERY_KUBERNETES_PORT
        self.port = int(self.port) if self.port else 19530
        self.kwargs = kwargs
        self.queue = queue.Queue()

        self.readonly_topo = readonly_topo

        if not self.namespace:
            self.namespace = open(incluster_namespace_path).read()

        kconfig.load_incluster_config(
        ) if self.in_cluster else kconfig.load_kube_config()
        self.v1 = client.CoreV1Api()

        self.listener = K8SEventListener(message_queue=self.queue,
                                         namespace=self.namespace,
                                         in_cluster=self.in_cluster,
                                         v1=self.v1,
                                         **kwargs)

        self.pod_heartbeater = K8SHeartbeatHandler(
            message_queue=self.queue,
            namespace=self.namespace,
            label_selector=self.label_selector,
            in_cluster=self.in_cluster,
            v1=self.v1,
            poll_interval=self.poll_interval,
            **kwargs)

        self.event_handler = EventHandler(mgr=self,
                                          message_queue=self.queue,
                                          namespace=self.namespace,
                                          pod_patt=self.pod_patt,
                                          **kwargs)

    def add_pod(self, name, ip):
        logger.debug('Register POD {} with IP {}'.format(
            name, ip))
        ok = True
        status = StatusType.OK
        try:
            uri = 'tcp://{}:{}'.format(ip, self.port)
            status, group = self.readonly_topo.create(name=name)
            if status == StatusType.OK:
                status, pool = group.create(name=name, uri=uri)
        except ConnectionConnectError as exc:
            ok = False
            logger.error('Connection error to: {}'.format(addr))

        # if ok and status == StatusType.OK:
        #     logger.info('KubernetesProvider Add Group \"{}\" Of 1 Address: {}'.format(name, uri))
        return ok

    def delete_pod(self, name):
        pool = self.readonly_topo.delete_group(name)
        return True

    def start(self):
        self.listener.daemon = True
        self.listener.start()
        self.event_handler.start()

        self.pod_heartbeater.start()
        return True

    def stop(self):
        self.listener.stop()
        self.pod_heartbeater.stop()
        self.event_handler.stop()

    @classmethod
    def Create(cls, readonly_topo, plugin_config, **kwargs):
        discovery = cls(config=plugin_config, readonly_topo=readonly_topo, **kwargs)
        return discovery


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(__file__, app.plugin_package_name))
    app.on_plugin_setup(KubernetesProvider)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))))
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))

    class Connect:
        def register(self, name, value):
            logger.error('Register: {} - {}'.format(name, value))

        def unregister(self, name):
            logger.error('Unregister: {}'.format(name))

        @property
        def conn_names(self):
            return set()

    connect_mgr = Connect()

    from discovery import DiscoveryConfig
    settings = DiscoveryConfig(DISCOVERY_KUBERNETES_NAMESPACE='xp',
                               DISCOVERY_KUBERNETES_POD_PATT=".*-ro-servers-.*",
                               DISCOVERY_KUBERNETES_LABEL_SELECTOR='tier=ro-servers',
                               DISCOVERY_KUBERNETES_POLL_INTERVAL=5,
                               DISCOVERY_KUBERNETES_IN_CLUSTER=False)

    provider_class = KubernetesProvider
    t = provider_class(conn_mgr=connect_mgr, plugin_config=settings)
    t.start()
    cnt = 100
    while cnt > 0:
        time.sleep(2)
        cnt -= 1
    t.stop()
