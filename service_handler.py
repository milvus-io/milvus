import logging

import grpco
from milvus.grpc_gen import milvus_pb2, milvus_pb2_grpc, status_pb2

logger = logging.getLogger(__name__)


class ServiceHandler(milvus_pb2_grpc.MilvusServiceServicer):
    def __init__(self, connections, *args, **kwargs):
        self.connections = self.connections
