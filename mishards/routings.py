import logging
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import and_

from mishards import exceptions, db
from mishards.hash_ring import HashRing
from mishards.models import Tables

logger = logging.getLogger(__name__)


class RouteManager:
    ROUTER_CLASSES = {}

    @classmethod
    def register_router_class(cls, target):
        name = target.__dict__.get('NAME', None)
        name = name if name else target.__class__.__name__
        cls.ROUTER_CLASSES[name] = target
        return target

    @classmethod
    def get_router_class(cls, name):
        return cls.ROUTER_CLASSES.get(name, None)


class RouterFactory:
    @classmethod
    def new_router(cls, name, conn_mgr, **kwargs):
        router_class = RouteManager.get_router_class(name)
        assert router_class
        return router_class(conn_mgr, **kwargs)


class RouterMixin:
    def __init__(self, conn_mgr):
        self.conn_mgr = conn_mgr

    def routing(self, table_name, metadata=None, **kwargs):
        raise NotImplemented()

    def connection(self, metadata=None):
        conn = self.conn_mgr.conn('WOSERVER', metadata=metadata)
        if conn:
            conn.on_connect(metadata=metadata)
        return conn.conn

    def query_conn(self, name, metadata=None):
        conn = self.conn_mgr.conn(name, metadata=metadata)
        if not conn:
            raise exceptions.ConnectionNotFoundError(name, metadata=metadata)
        conn.on_connect(metadata=metadata)
        return conn.conn


@RouteManager.register_router_class
class FileBasedHashRingRouter(RouterMixin):
    NAME = 'FileBasedHashRingRouter'

    def __init__(self, conn_mgr, **kwargs):
        super(FileBasedHashRingRouter, self).__init__(conn_mgr)

    def routing(self, table_name, metadata=None, **kwargs):
        range_array = kwargs.pop('range_array', None)
        return self._route(table_name, range_array, metadata, **kwargs)

    def _route(self, table_name, range_array, metadata=None, **kwargs):
        # PXU TODO: Implement Thread-local Context
        # PXU TODO: Session life mgt
        try:
            table = db.Session.query(Tables).filter(
                and_(Tables.table_id == table_name,
                     Tables.state != Tables.TO_DELETE)).first()
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise exceptions.DBError(message=str(e), metadata=metadata)

        if not table:
            raise exceptions.TableNotFoundError(table_name, metadata=metadata)
        files = table.files_to_search(range_array)
        db.remove_session()

        servers = self.conn_mgr.conn_names
        logger.info('Available servers: {}'.format(servers))

        ring = HashRing(servers)

        routing = {}

        for f in files:
            target_host = ring.get_node(str(f.id))
            sub = routing.get(target_host, None)
            if not sub:
                routing[target_host] = {'table_id': table_name, 'file_ids': []}
            routing[target_host]['file_ids'].append(str(f.id))

        return routing
