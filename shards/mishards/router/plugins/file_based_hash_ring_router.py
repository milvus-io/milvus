import logging
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import and_
from mishards.models import Tables
from mishards.router import RouterMixin
from mishards import exceptions, db
from mishards.hash_ring import HashRing

logger = logging.getLogger(__name__)


class Factory(RouterMixin):
    name = 'FileBasedHashRingRouter'

    def __init__(self, conn_mgr, **kwargs):
        super(Factory, self).__init__(conn_mgr)

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

    @classmethod
    def create(cls, config, **kwargs):
        conn_mgr = kwargs.pop('conn_mgr', None)
        if not conn_mgr:
            raise RuntimeError('Cannot find \'conn_mgr\' to initialize \'{}\''.format(self.name))
        router = cls(conn_mgr, **kwargs)
        return router


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(__file__, app.plugin_package_name))
    app.on_plugin_setup(Factory)
