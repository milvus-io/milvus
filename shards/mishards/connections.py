import logging
import threading
from functools import wraps
from milvus import Milvus
from milvus.client.hooks import BaseSearchHook

from mishards import (settings, exceptions)
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

    def on_duplicate_meta(self, name, url):
        if self.metas[name] == url:
            return self.on_same_meta(name, url)

        return self.on_diff_meta(name, url)

    def on_same_meta(self, name, url):
        # logger.warning('Register same meta: {}:{}'.format(name, url))
        pass

    def on_diff_meta(self, name, url):
        logger.warning('Received {} with diff url={}'.format(name, url))
        self.metas[name] = url
        self.conns[name] = {}

    def on_unregister_meta(self, name, url):
        logger.info('Unregister name={};url={}'.format(name, url))
        self.conns.pop(name, None)

    def on_nonexisted_meta(self, name):
        logger.warning('Non-existed meta: {}'.format(name))

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
