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

    # Declare and initialise readonly connection topology. At default, each ro node is
    # added into separate group.
    readonly_topo = ConnectionTopology()
    # Declare and initialise writable connection topology. At default, here is only one
    # writable node.
    writable_topo = ConnectionTopology()

    # Load discovery plugin. The default discovery is placed under `discovery/plugins`, where
    # provide two discovery plugins 'static' and 'kubernetes'.
    # In static plugin, the readonly node is added during initialization stage; in kubernetes
    # plugin, provider check readonly nodes health and listen change of nodes status to update
    # available readonly nodes in `readonly_topo`.
    from discovery.factory import DiscoveryFactory
    discover = DiscoveryFactory(config.DISCOVERY_PLUGIN_PATH).create(config.DISCOVERY_CLASS_NAME,
                                                                     readonly_topo=readonly_topo)

    from mishards.grpc_utils import GrpcSpanDecorator
    from tracer.factory import TracerFactory
    tracer = TracerFactory(config.TRACER_PLUGIN_PATH).create(config.TRACER_CLASS_NAME,
                                                             plugin_config=settings.TracingConfig,
                                                             span_decorator=GrpcSpanDecorator())

    # Load router plugin. The default is placed under `mishards/router/plugins`, where
    # provide two router plugins 'FileBasedHashRingRouter'.
    # Router selects available readonly nodes from `readonly_topo`.
    from mishards.router.factory import RouterFactory
    router = RouterFactory(config.ROUTER_PLUGIN_PATH).create(config.ROUTER_CLASS_NAME,
                                                             readonly_topo=readonly_topo,
                                                             writable_topo=writable_topo)

    # Server split search request into multiple sub request to readonly nodes, and rout the
    # other request to writable node.
    grpc_server.init_app(writable_topo=writable_topo,
                         readonly_topo=readonly_topo,
                         tracer=tracer,
                         router=router,
                         discover=discover,
                         max_workers=settings.MAX_WORKERS)

    from mishards import exception_handlers

    return grpc_server
