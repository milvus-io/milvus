import os
import sys
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import socket
from environs import Env
from mishards.exceptions import ConnectionConnectError
from mishards.topology import StatusType

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

    def __init__(self, config, readonly_topo, **kwargs):
        self.readonly_topo = readonly_topo
        hosts = map(str.strip, env.list('DISCOVERY_STATIC_HOSTS', []))
        self.port = env.int('DISCOVERY_STATIC_PORT', 19530)
        self.hosts = [resolve_address(host, self.port) for host in hosts]

    def start(self):
        ok = True
        for host in self.hosts:
            ok &= self.add_pod(host, host)
            if not ok: break
        if ok and len(self.hosts) == 0:
            logger.error('No address is specified')
            ok = False
        return ok

    def stop(self):
        for host in self.hosts:
            self.delete_pod(host)

    def add_pod(self, name, addr):
        ok = True
        status = StatusType.OK
        try:
            uri = 'tcp://{}'.format(addr)
            status, group = self.readonly_topo.create(name=name)
            if status == StatusType.OK:
                status, pool = group.create(name=name, uri=uri)
            if status not in (StatusType.OK, StatusType.DUPLICATED):
                ok = False
        except ConnectionConnectError as exc:
            ok = False
            logger.error('Connection error to: {}'.format(addr))

        if ok and status == StatusType.OK:
            logger.info('StaticDiscovery Add Static Group \"{}\" Of 1 Address: {}'.format(name, addr))
        return ok

    def delete_pod(self, name):
        pool = self.readonly_topo.delete_group(name)
        return True

    @classmethod
    def Create(cls, readonly_topo, plugin_config, **kwargs):
        discovery = cls(config=plugin_config, readonly_topo=readonly_topo, **kwargs)
        return discovery


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(__file__, app.plugin_package_name))
    app.on_plugin_setup(StaticDiscovery)
