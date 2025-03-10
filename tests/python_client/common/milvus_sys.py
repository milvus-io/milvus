import ujson
import json
from pymilvus.grpc_gen import milvus_pb2 as milvus_types
from pymilvus import connections
# from utils.util_log import test_log as log
sys_info_req = ujson.dumps({"metric_type": "system_info"})
sys_statistics_req = ujson.dumps({"metric_type": "system_statistics"})
sys_logs_req = ujson.dumps({"metric_type": "system_logs"})


class MilvusSys:
    def __init__(self, alias='default'):
        self.alias = alias
        self.handler = connections._fetch_handler(alias=self.alias)
        if self.handler is None:
            raise Exception(f"Connection {alias} is disconnected or nonexistent")

        # TODO: for now it only supports non_orm style API for getMetricsRequest
        req = milvus_types.GetMetricsRequest(request=sys_info_req)
        self.sys_info = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
        # req = milvus_types.GetMetricsRequest(request=sys_statistics_req)
        # self.sys_statistics = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
        # req = milvus_types.GetMetricsRequest(request=sys_logs_req)
        # self.sys_logs = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
        self.sys_info = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=60)
        # log.debug(f"sys_info: {self.sys_info}")

    def refresh(self):
        req = milvus_types.GetMetricsRequest(request=sys_info_req)
        self.sys_info = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
        # req = milvus_types.GetMetricsRequest(request=sys_statistics_req)
        # self.sys_statistics = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
        # req = milvus_types.GetMetricsRequest(request=sys_logs_req)
        # self.sys_logs = self.handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
        # log.debug(f"sys info response: {self.sys_info.response}")


    @property
    def system_version(self):
        """get the first node's build version as milvus build version"""
        return self.nodes[0].get('infos').get('system_info').get('system_version')

    @property
    def build_version(self):
        """get the first node's build version as milvus build version"""
        return self.nodes[0].get('infos').get('system_info').get('build_version')

    @property
    def build_time(self):
        """get the first node's build time as milvus build time"""
        return self.nodes[0].get('infos').get('system_info').get('build_time')

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
        for node in self.query_nodes:
            return node.get('infos').get('system_configurations').get('simd_type')
        raise Exception("No query node found")

    @property
    def query_nodes(self):
        """get all query nodes in Milvus deployment"""
        query_nodes = []
        for node in self.nodes:
            if 'querynode' == node.get('infos').get('type'):
                query_nodes.append(node)
        return query_nodes

    @property
    def data_nodes(self):
        """get all data nodes in Milvus deployment"""
        data_nodes = []
        for node in self.nodes:
            if 'datanode' == node.get('infos').get('type'):
                data_nodes.append(node)
        return data_nodes

    @property
    def proxy_nodes(self):
        """get all proxy nodes in Milvus deployment"""
        proxy_nodes = []
        for node in self.nodes:
            if 'proxy' == node.get('infos').get('type'):
                proxy_nodes.append(node)
        return proxy_nodes

    @property
    def nodes(self):
        """get all the nodes in Milvus deployment"""
        self.refresh()
        all_nodes = json.loads(self.sys_info.response).get('nodes_info')
        online_nodes = [node for node in all_nodes if node["infos"]["has_error"] is False]
        return online_nodes

    def get_nodes_by_type(self, node_type=None):
        """get milvus nodes by node type"""
        target_nodes = []
        if node_type is not None:
            for node in self.nodes:
                if str(node_type).lower() == str(node.get('infos').get('type')).lower():
                    target_nodes.append(node)
        return target_nodes


if __name__ == '__main__':
    uri = ""
    token = ""
    connections.connect(uri=uri, token=token)
    ms = MilvusSys()
    print(ms.build_version)
