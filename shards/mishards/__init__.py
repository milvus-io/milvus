import logging
from mishards import settings
logger = logging.getLogger()

from mishards.db_base import DB
db = DB()

from mishards.server import Server
grpc_server = Server()


def create_app(testing_config=None):
    config = testing_config if testing_config else settings.DefaultConfig
    db.init_db(uri=config.SQLALCHEMY_DATABASE_URI, echo=config.SQL_ECHO, pool_size=config.SQL_POOL_SIZE,
               pool_recycle=config.SQL_POOL_RECYCLE, pool_timeout=config.SQL_POOL_TIMEOUT,
               pool_pre_ping=config.SQL_POOL_PRE_PING, max_overflow=config.SQL_MAX_OVERFLOW)

    from mishards.connections import ConnectionTopology

    readonly_topo = ConnectionTopology()
    writable_topo = ConnectionTopology()

    from discovery.factory import DiscoveryFactory
    discover = DiscoveryFactory(config.DISCOVERY_PLUGIN_PATH).create(config.DISCOVERY_CLASS_NAME,
                                                                     readonly_topo=readonly_topo)

    from mishards.grpc_utils import GrpcSpanDecorator
    from tracer.factory import TracerFactory
    tracer = TracerFactory(config.TRACER_PLUGIN_PATH).create(config.TRACER_CLASS_NAME,
                                                             plugin_config=settings.TracingConfig,
                                                             span_decorator=GrpcSpanDecorator())

    from mishards.router.factory import RouterFactory
    router = RouterFactory(config.ROUTER_PLUGIN_PATH).create(config.ROUTER_CLASS_NAME,
                                                             readonly_topo=readonly_topo,
                                                             writable_topo=writable_topo)

    grpc_server.init_app(writable_topo=writable_topo,
                         readonly_topo=readonly_topo,
                         tracer=tracer,
                         router=router,
                         discover=discover,
                         max_workers=settings.MAX_WORKERS)

    from mishards import exception_handlers

    return grpc_server
