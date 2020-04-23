import time
import enum
import json
import logging
import threading
from functools import wraps
from collections import defaultdict
from milvus import Milvus
from milvus.client.hooks import BaseSearchHook

from mishards import (settings, exceptions, topology)
from utils import singleton

logger = logging.getLogger(__name__)


class Searchook(BaseSearchHook):

    def on_response(self, *args, **kwargs):
        return True


class Connection:
    def __init__(self, name, uri, max_retry=1, error_handlers=None, **kwargs):
        self.name = name
        self.uri = uri
        self.max_retry = max_retry
        self.retried = 0
        self.conn = Milvus()
        self.error_handlers = [] if not error_handlers else error_handlers
        self.on_retry_func = kwargs.get('on_retry_func', None)

        # define search hook
        self.conn.set_hook(search_in_file=Searchook())
        # self._connect()

    def __str__(self):
        return 'Connection:name=\"{}\";uri=\"{}\"'.format(self.name, self.uri)

    def _connect(self, metadata=None):
        try:
            self.conn.connect(uri=self.uri)
        except Exception as e:
            if not self.error_handlers:
                raise exceptions.ConnectionConnectError(message=str(e), metadata=metadata)
            for handler in self.error_handlers:
                handler(e, metadata=metadata)

    @property
    def can_retry(self):
        return self.retried < self.max_retry

    @property
    def connected(self):
        return self.conn.connected()

    def on_retry(self):
        if self.on_retry_func:
            self.on_retry_func(self)
        else:
            self.retried > 1 and logger.warning('{} is retrying {}'.format(self, self.retried))

    def on_connect(self, metadata=None):
        while not self.connected and self.can_retry:
            self.retried += 1
            self.on_retry()
            self._connect(metadata=metadata)

        if not self.can_retry and not self.connected:
            raise exceptions.ConnectionConnectError(message='Max retry {} reached!'.format(self.max_retry,
                                                                                           metadata=metadata))

        self.retried = 0

    def connect(self, func, exception_handler=None):
        @wraps(func)
        def inner(*args, **kwargs):
            self.on_connect()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if exception_handler:
                    exception_handler(e)
                else:
                    raise e
        return inner

    def __str__(self):
        return '<Connection: {}:{}>'.format(self.name, id(self))

    def __repr__(self):
        return self.__str__()


class Duration:
    def __init__(self):
        self.start_ts = time.time()
        self.end_ts = None

    def stop(self):
        if self.end_ts:
            return False

        self.end_ts = time.time()
        return True

    @property
    def value(self):
        if not self.end_ts:
            return None

        return self.end_ts - self.start_ts


class ProxyMixin:
    def __getattr__(self, name):
        target = self.__dict__.get(name, None)
        if target or not self.connection:
            return target
        return getattr(self.connection, name)


class ScopedConnection(ProxyMixin):
    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection
        self.duration = Duration()

    def __del__(self):
        self.release()

    def __str__(self):
        return self.connection.__str__()

    def release(self):
        if not self.pool or not self.connection:
            return
        self.pool.release(self.connection)
        self.duration.stop()
        self.pool.record_duration(self.connection, self.duration)
        self.pool = None
        self.connection = None


class ConnectionPool(topology.TopoObject):
    def __init__(self, name, uri, max_retry=1, capacity=-1, **kwargs):
        super().__init__(name)
        self.capacity = capacity
        self.pending_pool = set()
        self.active_pool = set()
        self.connection_ownership = {}
        self.uri = uri
        self.max_retry = max_retry
        self.kwargs = kwargs
        self.cv = threading.Condition()
        self.durations = defaultdict(list)

    def record_duration(self, conn, duration):
        if len(self.durations[conn]) >= 10000:
            self.durations[conn].pop(0)

        self.durations[conn].append(duration)

    def stats(self):
        out = {'connections': {}}
        connections = out['connections']
        take_time = []
        for conn, durations in self.durations.items():
            total_time = sum(d.value for d in durations)
            connections[id(conn)] = {
                'total_time': total_time,
                'called_times': len(durations)
            }
            take_time.append(total_time)

        out['max-time'] = max(take_time)
        out['num'] = len(self.durations)
        logger.debug(json.dumps(out, indent=2))
        return out

    def __len__(self):
        return len(self.pending_pool) + len(self.active_pool)

    @property
    def active_num(self):
        return len(self.active_pool)

    def _is_full(self):
        if self.capacity < 0:
            return False
        return len(self) >= self.capacity

    def fetch(self, timeout=1):
        with self.cv:
            timeout_times = 0
            while (len(self.pending_pool) == 0 and self._is_full() and timeout_times < 1):
                self.cv.notifyAll()
                self.cv.wait(timeout)
                timeout_times += 1

            connection = None
            if timeout_times >= 1:
                return connection

            # logger.error('[Connection] Pool \"{}\" SIZE={} ACTIVE={}'.format(self.name, len(self), self.active_num))
            if len(self.pending_pool) == 0:
                connection = self.create()
            else:
                connection = self.pending_pool.pop()
            # logger.debug('[Connection] Registerring \"{}\" into pool \"{}\"'.format(connection, self.name))
            self.active_pool.add(connection)
        scoped_connection = ScopedConnection(self, connection)
        return scoped_connection

    def release(self, connection):
        with self.cv:
            if connection not in self.active_pool:
                raise RuntimeError('\"{}\" not found in pool \"{}\"'.format(connection, self.name))
            # logger.debug('[Connection] Releasing \"{}\" from pool \"{}\"'.format(connection, self.name))
            # logger.debug('[Connection] Pool \"{}\" SIZE={} ACTIVE={}'.format(self.name, len(self), self.active_num))
            self.active_pool.remove(connection)
            self.pending_pool.add(connection)

    def create(self):
        connection = Connection(name=self.name, uri=self.uri, max_retry=self.max_retry, **self.kwargs)
        return connection


class ConnectionGroup(topology.TopoGroup):
    def __init__(self, name):
        super().__init__(name)

    def stats(self):
        out = {}
        for name, item in self.items.items():
            out[name] = item.stats()

        return out

    def on_pre_add(self, topo_object):
        conn = topo_object.fetch()
        conn.on_connect(metadata=None)
        status, version = conn.conn.server_version()
        if not status.OK():
            logger.error('Cannot connect to newly added address: {}. Remove it now'.format(topo_object.name))
            return False
        if version not in settings.SERVER_VERSIONS:
            logger.error('Cannot connect to server of version: {}. Only {} supported'.format(version,
                settings.SERVER_VERSIONS))
            return False

        return True

    def create(self, name, **kwargs):
        uri = kwargs.get('uri', None)
        if not uri:
            raise RuntimeError('\"uri\" is required to create connection pool')
        pool = ConnectionPool(name=name, **kwargs)
        status = self.add(pool)
        if status != topology.StatusType.OK:
            pool = None
        return status, pool


class ConnectionTopology(topology.Topology):
    def __init__(self):
        super().__init__()

    def stats(self):
        out = {}
        for name, group in self.topo_groups.items():
            out[name] = group.stats()

        return out

    def create(self, name):
        group = ConnectionGroup(name)
        status = self.add_group(group)
        if status == topology.StatusType.DUPLICATED:
            group = None
        return status, group
