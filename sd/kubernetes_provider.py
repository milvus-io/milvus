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
from kubernetes import client, config, watch

from utils import singleton
from sd import ProviderManager

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
            config.load_incluster_config(
            ) if self.in_cluster else config.load_kube_config()
            self.v1 = client.CoreV1Api()


class K8SHeartbeatHandler(threading.Thread, K8SMixin):
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
    def __init__(self, mgr, message_queue, namespace, pod_patt, **kwargs):
        threading.Thread.__init__(self)
        self.mgr = mgr
        self.queue = message_queue
        self.kwargs = kwargs
        self.terminate = False
        self.pod_patt = re.compile(pod_patt)
        self.namespace = namespace

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
                logger.error('Pod {} is started but cannot read pod'.format(
                    event['pod']))
            return
        elif try_cnt <= 0 and not pod.status.pod_ip:
            logger.warn('NoPodIPFoundError')
            return

        logger.info('Register POD {} with IP {}'.format(
            pod.metadata.name, pod.status.pod_ip))
        self.mgr.add_pod(name=pod.metadata.name, ip=pod.status.pod_ip)

    def on_pod_killing(self, event, **kwargs):
        logger.info('Unregister POD {}'.format(event['pod']))
        self.mgr.delete_pod(name=event['pod'])

    def on_pod_heartbeat(self, event, **kwargs):
        names = self.mgr.conn_mgr.conn_names

        running_names = set()
        for each_event in event['events']:
            if each_event['ready']:
                self.mgr.add_pod(name=each_event['pod'], ip=each_event['ip'])
                running_names.add(each_event['pod'])
            else:
                self.mgr.delete_pod(name=each_event['pod'])

        to_delete = names - running_names
        for name in to_delete:
            self.mgr.delete_pod(name)

        logger.info(self.mgr.conn_mgr.conn_names)

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
                 poll_interval, **kwargs):
        self.namespace = namespace
        self.pod_patt = pod_patt
        self.label_selector = label_selector
        self.in_cluster = in_cluster
        self.poll_interval = poll_interval


@singleton
@ProviderManager.register_service_provider
class KubernetesProvider(object):
    NAME = 'Kubernetes'

    def __init__(self, settings, conn_mgr, **kwargs):
        self.namespace = settings.namespace
        self.pod_patt = settings.pod_patt
        self.label_selector = settings.label_selector
        self.in_cluster = settings.in_cluster
        self.poll_interval = settings.poll_interval
        self.kwargs = kwargs
        self.queue = queue.Queue()

        self.conn_mgr = conn_mgr

        if not self.namespace:
            self.namespace = open(incluster_namespace_path).read()

        config.load_incluster_config(
        ) if self.in_cluster else config.load_kube_config()
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
        self.conn_mgr.register(name, 'tcp://{}:19530'.format(ip))

    def delete_pod(self, name):
        self.conn_mgr.unregister(name)

    def start(self):
        self.listener.daemon = True
        self.listener.start()
        self.event_handler.start()

        self.pod_heartbeater.start()

    def stop(self):
        self.listener.stop()
        self.pod_heartbeater.stop()
        self.event_handler.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    class Connect:
        def register(self, name, value):
            logger.error('Register: {} - {}'.format(name, value))

        def unregister(self, name):
            logger.error('Unregister: {}'.format(name))

        @property
        def conn_names(self):
            return set()

    connect_mgr = Connect()

    settings = KubernetesProviderSettings(namespace='xp',
                                          pod_patt=".*-ro-servers-.*",
                                          label_selector='tier=ro-servers',
                                          poll_interval=5,
                                          in_cluster=False)

    provider_class = ProviderManager.get_provider('Kubernetes')
    t = provider_class(conn_mgr=connect_mgr, settings=settings)
    t.start()
    cnt = 100
    while cnt > 0:
        time.sleep(2)
        cnt -= 1
    t.stop()
