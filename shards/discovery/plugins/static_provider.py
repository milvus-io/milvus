import os
import sys
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import socket

logger = logging.getLogger(__name__)


class StaticDiscovery(object):
    name = 'static'

    def __init__(self, config, conn_mgr, **kwargs):
        self.conn_mgr = conn_mgr
        hosts = [config.DISCOVERY_STATIC_HOSTS] if isinstance(config.DISCOVERY_STATIC_HOSTS, str) else hosts
        self.hosts = [socket.gethostbyname(host) for host in hosts]
        self.port = config.DISCOVERY_STATIC_PORT

    def start(self):
        for host in self.hosts:
            self.add_pod(host, host)

    def stop(self):
        for host in self.hosts:
            self.delete_pod(host)

    def add_pod(self, name, ip):
        self.conn_mgr.register(name, 'tcp://{}:{}'.format(ip, self.port))

    def delete_pod(self, name):
        self.conn_mgr.unregister(name)

    @classmethod
    def Create(cls, conn_mgr, plugin_config, **kwargs):
        discovery = cls(config=plugin_config, conn_mgr=conn_mgr, **kwargs)
        return discovery


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(__file__, app.plugin_package_name))
    app.on_plugin_setup(StaticDiscovery)
