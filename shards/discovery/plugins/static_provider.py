import os
import sys
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import socket
from environs import Env

logger = logging.getLogger(__name__)
env = Env()

DELIMITER = ':'

def parse_host(addr):
    splited_arr = addr.split(DELIMITER)
    return splited_arr

def resolve_address(addr, default_port):
    addr_arr = parse_host(addr)
    assert len(addr_arr) >= 1 and len(addr_arr) <= 2, 'Invalid Addr: {}'.format(addr)
    port = addr_arr[1] if len(addr_arr) == 2 else default_port
    return '{}:{}'.format(socket.gethostbyname(addr_arr[0]), port)

class StaticDiscovery(object):
    name = 'static'

    def __init__(self, config, conn_mgr, **kwargs):
        self.conn_mgr = conn_mgr
        hosts = env.list('DISCOVERY_STATIC_HOSTS', [])
        self.port = env.int('DISCOVERY_STATIC_PORT', 19530)
        self.hosts = [resolve_address(host, self.port) for host in hosts]

    def start(self):
        for host in self.hosts:
            self.add_pod(host, host)

    def stop(self):
        for host in self.hosts:
            self.delete_pod(host)

    def add_pod(self, name, addr):
        self.conn_mgr.register(name, 'tcp://{}'.format(addr))
        logger.debug('StaticDiscovery Add Static Address: {}'.format(addr))

    def delete_pod(self, name):
        self.conn_mgr.unregister(name)

    @classmethod
    def Create(cls, conn_mgr, plugin_config, **kwargs):
        discovery = cls(config=plugin_config, conn_mgr=conn_mgr, **kwargs)
        return discovery


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(__file__, app.plugin_package_name))
    app.on_plugin_setup(StaticDiscovery)
