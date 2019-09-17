import logging
from milvus import Milvus
from functools import wraps
from contextlib import contextmanager

import exceptions

logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, name, uri, max_retry=1, error_handlers=None, **kwargs):
        self.name = name
        self.uri = uri
        self.max_retry = max_retry
        self.retried = 0
        self.conn = Milvus()
        self.error_handlers = [] if not error_handlers else error_handlers
        self.on_retry_func = kwargs.get('on_retry_func', None)

    def __str__(self):
        return 'Connection:name=\"{}\";uri=\"{}\"'.format(self.name, self.uri)

    def _connect(self):
        try:
            self.conn.connect(uri=self.uri)
        except Exception as e:
            if not self.error_handlers:
                raise exceptions.ConnectionConnectError(message='')
            for handler in self.error_handlers:
                handler(e)

    @property
    def can_retry(self):
        return self.retried <= self.max_retry

    @property
    def connected(self):
        return self.conn.connected()

    def on_retry(self):
        if self.on_retry_func:
            self.on_retry_func(self)
        else:
            logger.warn('{} is retrying {}'.format(self, self.retried))

    def on_connect(self):
        while not self.connected and self.can_retry:
            self.retried += 1
            self.on_retry()
            self._connect()

        if not self.can_retry and not self.connected:
            raise exceptions.ConnectionConnectError(message='Max retry {} reached!'.format(self.max_retry))

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

if __name__ == '__main__':
    class Conn:
        def __init__(self, state):
            self.state = state

        def connect(self, uri):
            return self.state

        def connected(self):
            return self.state

    fail_conn = Conn(False)
    success_conn = Conn(True)

    class Retry:
        def __init__(self):
            self.times = 0

        def __call__(self, conn):
            self.times += 1
            print('Retrying {}'.format(self.times))


    retry_obj = Retry()
    c = Connection('client', uri='localhost', on_retry_func=retry_obj)
    c.conn = fail_conn

    def f():
        print('ffffffff')

    # m = c.connect(func=f)
    # m()

    c.conn = success_conn
    m = c.connect(func=f)
    m()
