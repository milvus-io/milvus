import logging
import grpc
import time
import socket
import inspect
from urllib.parse import urlparse
from functools import wraps
from concurrent import futures
from grpc._cython import cygrpc
from milvus.grpc_gen.milvus_pb2_grpc import add_MilvusServiceServicer_to_server
from mishards.service_handler import ServiceHandler
from mishards import settings, discover

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, conn_mgr, port=19530, max_workers=10, **kwargs):
        self.pre_run_handlers = set()
        self.error_handler = {}
        self.exit_flag = False
        self.port = int(port)
        self.conn_mgr = conn_mgr
        self.server_impl = grpc.server(
            thread_pool=futures.ThreadPoolExecutor(max_workers=max_workers),
            options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                (cygrpc.ChannelArgKey.max_receive_message_length, -1)]
        )

        self.register_pre_run_handler(self.pre_run_handler)

    def pre_run_handler(self):
        woserver = settings.WOSERVER if not settings.TESTING else settings.TESTING_WOSERVER
        url = urlparse(woserver)
        ip = socket.gethostbyname(url.hostname)
        socket.inet_pton(socket.AF_INET, ip)
        self.conn_mgr.register('WOSERVER',
                '{}://{}:{}'.format(url.scheme, ip, url.port))

    def register_pre_run_handler(self, func):
        logger.info('Regiterring {} into server pre_run_handlers'.format(func))
        self.pre_run_handlers.add(func)
        return func

    def errorhandler(self, exception):
        if inspect.isclass(exception) and issubclass(exception, Exception):
            def wrapper(func):
                self.error_handlers[exception] = func
                return func
            return wrapper
        return exception

    def on_pre_run(self):
        for handler in self.pre_run_handlers:
            handler()
        discover.start()

    def start(self, port=None):
        add_MilvusServiceServicer_to_server(ServiceHandler(conn_mgr=self.conn_mgr), self.server_impl)
        self.server_impl.add_insecure_port("[::]:{}".format(str(port or self._port)))
        self.server_impl.start()

    def run(self, port):
        logger.info('Milvus server start ......')
        port = port or self.port
        self.on_pre_run()

        self.start(port)
        logger.info('Successfully')
        logger.info('Listening on port {}'.format(port))

        try:
            while not self.exit_flag:
                time.sleep(5)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        logger.info('Server is shuting down ......')
        self.exit_flag = True
        self.server_impl.stop(0)
        logger.info('Server is closed')
