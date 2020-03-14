import logging
import threading
import enum
from functools import wraps
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

    def __del__(self):
        self.release()

    def __str__(self):
        return self.connection.__str__()

    def release(self):
        if not self.pool or not self.connection:
            return
        self.pool.release(self.connection)
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

            # logger.debug('[Connection] Pool \"{}\" SIZE={} ACTIVE={}'.format(self.name, len(self), self.active_num))
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

    def create(self, name):
        group = ConnectionGroup(name)
        status = self.add_group(group)
        if status == topology.StatusType.DUPLICATED:
            group = None
        return status, group


@singleton
class ConnectionMgr:
    def __init__(self):
        self.metas = {}
        self.conns = {}

    @property
    def conn_names(self):
        return set(self.metas.keys()) - set(['WOSERVER'])

    def conn(self, name, metadata, throw=False):
        c = self.conns.get(name, None)
        if not c:
            url = self.metas.get(name, None)
            if not url:
                if not throw:
                    return None
                raise exceptions.ConnectionNotFoundError(message='Connection {} not found'.format(name),
                                                         metadata=metadata)
            this_conn = Connection(name=name, uri=url, max_retry=settings.MAX_RETRY)
            threaded = {
                threading.get_ident(): this_conn
            }
            self.conns[name] = threaded
            return this_conn

        tid = threading.get_ident()
        rconn = c.get(tid, None)
        if not rconn:
            url = self.metas.get(name, None)
            if not url:
                if not throw:
                    return None
                raise exceptions.ConnectionNotFoundError('Connection {} not found'.format(name),
                                                         metadata=metadata)
            this_conn = Connection(name=name, uri=url, max_retry=settings.MAX_RETRY)
            c[tid] = this_conn
            return this_conn

        return rconn

    def on_new_meta(self, name, url):
        logger.info('Register Connection: name={};url={}'.format(name, url))
        self.metas[name] = url
        conn = self.conn(name, metadata=None)
        conn.on_connect(metadata=None)
        status, _ = conn.conn.server_version()
        if not status.OK():
            logger.error('Cannot connect to newly added address: {}. Remove it now'.format(name))
            self.unregister(name)
            return False
        return True

    def on_duplicate_meta(self, name, url):
        if self.metas[name] == url:
            return self.on_same_meta(name, url)

        return self.on_diff_meta(name, url)

    def on_same_meta(self, name, url):
        # logger.warning('Register same meta: {}:{}'.format(name, url))
        return True

    def on_diff_meta(self, name, url):
        logger.warning('Received {} with diff url={}'.format(name, url))
        self.metas[name] = url
        self.conns[name] = {}
        return True

    def on_unregister_meta(self, name, url):
        logger.info('Unregister name={};url={}'.format(name, url))
        self.conns.pop(name, None)
        return True

    def on_nonexisted_meta(self, name):
        logger.warning('Non-existed meta: {}'.format(name))
        return False

    def register(self, name, url):
        meta = self.metas.get(name)
        if not meta:
            return self.on_new_meta(name, url)
        else:
            return self.on_duplicate_meta(name, url)

    def unregister(self, name):
        logger.info('Unregister Connection: name={}'.format(name))
        url = self.metas.pop(name, None)
        if url is None:
            return self.on_nonexisted_meta(name)
        return self.on_unregister_meta(name, url)
