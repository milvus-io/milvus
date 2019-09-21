import os, sys
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import singleton
from sd import ProviderManager

class StaticProviderSettings:
    def __init__(self, hosts):
        self.hosts = hosts

@singleton
@ProviderManager.register_service_provider
class KubernetesProvider(object):
    NAME = 'Static'
    def __init__(self, settings, conn_mgr, **kwargs):
        self.conn_mgr = conn_mgr
        self.hosts = settings.hosts

    def start(self):
        for host in self.hosts:
            self.add_pod(host, host)

    def stop(self):
        for host in self.hosts:
            self.delete_pod(host)

    def add_pod(self, name, ip):
        self.conn_mgr.register(name, 'tcp://{}:19530'.format(ip))

    def delete_pod(self, name):
        self.conn_mgr.unregister(name)
