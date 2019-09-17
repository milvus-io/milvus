import logging
import grpc
import time
from concurrent import futures
from grpc._cython import cygrpc
from milvus.grpc_gen.milvus_pb2_grpc import add_MilvusServiceServicer_to_server
from service_handler import ServiceHandler
import settings

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, conn_mgr, port=19530, max_workers=10, **kwargs):
        self.exit_flag = False
        self.port = int(port)
        self.conn_mgr = conn_mgr
        self.server_impl = grpc.server(
            thread_pool=futures.ThreadPoolExecutor(max_workers=max_workers),
            options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                (cygrpc.ChannelArgKey.max_receive_message_length, -1)]
        )

    def start(self, port=None):
        add_MilvusServiceServicer_to_server(ServiceHandler(conn_mgr=self.conn_mgr), self.server_impl)
        self.server_impl.add_insecure_port("[::]:{}".format(str(port or self._port)))
        self.server_impl.start()

    def run(self, port):
        logger.info('Milvus server start ......')
        port = port or self.port

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
        self.server.stop(0)
        logger.info('Server is closed')
