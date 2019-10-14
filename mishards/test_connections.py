import logging
import pytest

from mishards.connections import (ConnectionMgr, Connection)
from mishards import exceptions

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures('app')
class TestConnection:
    def test_manager(self):
        mgr = ConnectionMgr()

        mgr.register('pod1', '111')
        mgr.register('pod2', '222')
        mgr.register('pod2', '222')
        mgr.register('pod2', '2222')
        assert len(mgr.conn_names) == 2

        mgr.unregister('pod1')
        assert len(mgr.conn_names) == 1

        mgr.unregister('pod2')
        assert len(mgr.conn_names) == 0

        mgr.register('WOSERVER', 'xxxx')
        assert len(mgr.conn_names) == 0

    def test_connection(self):
        class Conn:
            def __init__(self, state):
                self.state = state

            def connect(self, uri):
                return self.state

            def connected(self):
                return self.state
        FAIL_CONN = Conn(False)
        PASS_CONN = Conn(True)

        class Retry:
            def __init__(self):
                self.times = 0

            def __call__(self, conn):
                self.times += 1
                logger.info('Retrying {}'.format(self.times))

        class Func():
            def __init__(self):
                self.executed = False

            def __call__(self):
                self.executed = True

        max_retry = 3

        RetryObj = Retry()
        c = Connection('client', uri='',
                       max_retry=max_retry,
                       on_retry_func=RetryObj)
        c.conn = FAIL_CONN
        ff = Func()
        this_connect = c.connect(func=ff)
        with pytest.raises(exceptions.ConnectionConnectError):
            this_connect()
        assert RetryObj.times == max_retry
        assert not ff.executed
        RetryObj = Retry()

        c.conn = PASS_CONN
        this_connect = c.connect(func=ff)
        this_connect()
        assert ff.executed
        assert RetryObj.times == 0
