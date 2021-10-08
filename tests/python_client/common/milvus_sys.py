import ujson
import json
from pymilvus.grpc_gen import milvus_pb2 as milvus_types
from pymilvus import connections

sys_info_req = ujson.dumps({"metric_type": "system_info"})
sys_statistics_req = ujson.dumps({"metric_type": "system_statistics"})
sys_logs_req = ujson.dumps({"metric_type": "system_logs"})


class MilvusSys:
    def __init__(self, alias):
        self.alias = alias
        self.client = connections.get_connection(alias=self.alias)
        if self.client is None:
            raise Exception(f"Connection {alias} is disconnected or nonexistent")

        # TODO: for now it only support non_orm style API for getMetricsRequest
        with self.client._connection() as handler:
            req = milvus_types.GetMetricsRequest(request=sys_info_req)
            self.sys_info = handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
            req = milvus_types.GetMetricsRequest(request=sys_statistics_req)
            self.sys_statistics = handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
            req = milvus_types.GetMetricsRequest(request=sys_logs_req)
            self.sys_logs = handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)

    @property
    def build_version(self):
        """get the first node's build version as milvus build version"""
        return self.nodes[0].get('infos').get('system_info').get('system_version')

    @property
    def deploy_mode(self):
        """get the first node's deploy_mode as milvus deploy_mode"""
        return self.nodes[0].get('infos').get('system_info').get('deploy_mode')

    @property
    def simd_type(self):
        """
        get simd type that milvus is running against
        return the first query node's simd type
        """
        for node in self.nodes:
            if 'QueryNode' == node.get('infos').get('type'):
                return node.get('infos').get('system_configurations').get('simd_type')
        raise Exception("No query node found")

    @property
    def query_nodes(self):
        """get all query nodes in Milvus deployment"""
        query_nodes = []
        for node in self.nodes:
            if 'QueryNode' == node.get('infos').get('type'):
                query_nodes.append(node)
        return query_nodes

    @property
    def data_nodes(self):
        """get all data nodes in Milvus deployment"""
        data_nodes = []
        for node in self.nodes:
            if 'DataNode' == node.get('infos').get('type'):
                data_nodes.append(node)
        return data_nodes

    @property
    def index_nodes(self):
        """get all index nodes in Milvus deployment"""
        index_nodes = []
        for node in self.nodes:
            if 'IndexNode' == node.get('infos').get('type'):
                index_nodes.append(node)
        return index_nodes

    @property
    def nodes(self):
        """get all the nodes in Milvus deployment"""
        return json.loads(self.sys_info.response).get('nodes_info')

    def get_nodes_by_type(self, node_type=None):
        """get milvus nodes by node type"""
        target_nodes = []
        if node_type is not None:
            for node in self.nodes:
                if str(node_type).lower() == str(node.get('infos').get('type')).lower():
                    target_nodes.append(node)
        return target_nodes

