import logging
from mishards import settings
logger = logging.getLogger()

from mishards.db_base import DB
db = DB()

from mishards.server import Server
grpc_server = Server()


def create_app(testing_config=None):
    config = testing_config if testing_config else settings.DefaultConfig
    db.init_db(uri=config.SQLALCHEMY_DATABASE_URI, echo=config.SQL_ECHO)

    from mishards.connections import ConnectionMgr
    connect_mgr = ConnectionMgr()

    from discovery.factory import DiscoveryFactory
    discover = DiscoveryFactory(config.DISCOVERY_PLUGIN_PATH).create(config.DISCOVERY_CLASS_NAME,
                                                                     conn_mgr=connect_mgr)

    from mishards.grpc_utils import GrpcSpanDecorator
    from tracer.factory import TracerFactory
    tracer = TracerFactory(config.TRACER_PLUGIN_PATH).create(config.TRACER_CLASS_NAME,
                                                             plugin_config=settings.TracingConfig,
                                                             span_decorator=GrpcSpanDecorator())

    from mishards.router.factory import RouterFactory
    router = RouterFactory(config.ROUTER_PLUGIN_PATH).create(config.ROUTER_CLASS_NAME,
                                                             conn_mgr=connect_mgr)

    grpc_server.init_app(conn_mgr=connect_mgr,
                         tracer=tracer,
                         router=router,
                         discover=discover)

    from mishards import exception_handlers

    return grpc_server
