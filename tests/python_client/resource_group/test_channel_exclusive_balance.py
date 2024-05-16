import pytest
import time
from typing import Union, List
from pymilvus import connections, utility, Collection
from pymilvus.client.constants import DEFAULT_RESOURCE_GROUP
from pymilvus.client.types import ResourceGroupConfig, ResourceGroupInfo
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
from chaos.checker import (InsertChecker,
                           UpsertChecker,
                           SearchChecker,
                           HybridSearchChecker,
                           QueryChecker,
                           DeleteChecker,
                           Op,
                           ResultAnalyzer
                           )
from chaos import chaos_commons as cc
from common import common_func as cf
from utils.util_k8s import get_querynode_id_pod_pairs
from common import common_type as ct
from customize.milvus_operator import MilvusOperator
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel
from chaos.chaos_commons import assert_statistic
from delayed_assert import assert_expectations

namespace = 'chaos-testing'
prefix = "test_rg"

from rich.table import Table
from rich.console import Console


def display_resource_group_info(info: Union[ResourceGroupInfo, List[ResourceGroupInfo]]):
    table = Table(title="Resource Group Info")
    table.width = 200
    table.add_column("Name", style="cyan")
    table.add_column("Capacity", style="cyan")
    table.add_column("Available Node", style="cyan")
    table.add_column("Loaded Replica", style="cyan")
    table.add_column("Outgoing Node", style="cyan")
    table.add_column("Incoming Node", style="cyan")
    table.add_column("Request", style="cyan")
    table.add_column("Limit", style="cyan")
    table.add_column("Nodes", style="cyan")
    if isinstance(info, list):
        for i in info:
            table.add_row(
                i.name,
                str(i.capacity),
                str(i.num_available_node),
                str(i.num_loaded_replica),
                str(i.num_outgoing_node),
                str(i.num_incoming_node),
                str(i.config.requests.node_num),
                str(i.config.limits.node_num),
                "\n".join([str(node.hostname) for node in i.nodes])
            )
    else:
        table.add_row(
            info.name,
            str(info.capacity),
            str(info.num_available_node),
            str(info.num_loaded_replica),
            str(info.num_outgoing_node),
            str(info.num_incoming_node),
            str(info.config.requests.node_num),
            str(info.config.limits.node_num),
            "\n".join([str(node.hostname) for node in info.nodes])
        )

    console = Console()
    console.width = 300
    console.print(table)


def display_segment_distribution_info(collection_name, release_name):
    table = Table(title=f"{collection_name} Segment Distribution Info")
    table.width = 200
    table.add_column("Segment ID", style="cyan")
    table.add_column("Collection ID", style="cyan")
    table.add_column("Partition ID", style="cyan")
    table.add_column("Num Rows", style="cyan")
    table.add_column("State", style="cyan")
    table.add_column("Node ID", style="cyan")
    table.add_column("Node Name", style="cyan")
    res = utility.get_query_segment_info(collection_name)
    label = f"app.kubernetes.io/instance={release_name}, app.kubernetes.io/component=querynode"
    querynode_id_pod_pair = get_querynode_id_pod_pairs("chaos-testing", label)

    for r in res:
        table.add_row(
            str(r.segmentID),
            str(r.collectionID),
            str(r.partitionID),
            str(r.num_rows),
            str(r.state),
            str(r.nodeIds),
            str([querynode_id_pod_pair.get(node_id) for node_id in r.nodeIds])
        )
    console = Console()
    console.width = 300
    console.print(table)


def list_all_resource_groups():
    rg_names = utility.list_resource_groups()
    resource_groups = []
    for rg_name in rg_names:
        resource_group = utility.describe_resource_group(rg_name)
        resource_groups.append(resource_group)
    display_resource_group_info(resource_groups)


def _install_milvus(image_tag="master-latest"):
    release_name = f"rg-test-{cf.gen_digits_by_length(6)}"
    cus_configs = {'spec.mode': 'cluster',
                   'spec.dependencies.msgStreamType': 'kafka',
                   'spec.components.image': f'harbor.milvus.io/milvus/milvus:{image_tag}',
                   'metadata.namespace': namespace,
                   'metadata.name': release_name,
                   'spec.components.proxy.serviceType': 'LoadBalancer',
                   }
    milvus_op = MilvusOperator()
    log.info(f"install milvus with configs: {cus_configs}")
    milvus_op.install(cus_configs)
    healthy = milvus_op.wait_for_healthy(release_name, namespace, timeout=1200)
    log.info(f"milvus healthy: {healthy}")
    if healthy:
        endpoint = milvus_op.endpoint(release_name, namespace).split(':')
        log.info(f"milvus endpoint: {endpoint}")
        host = endpoint[0]
        port = endpoint[1]
        return release_name, host, port
    else:
        return release_name, None, None


class TestChannelExclusiveBalance(TestcaseBase):

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)
        connections.disconnect("default")
        connections.remove_connection("default")

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        shards_num = 5
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name, shards_num=shards_num),
            Op.upsert: UpsertChecker(collection_name=c_name, shards_num=shards_num),
            Op.search: SearchChecker(collection_name=c_name, shards_num=shards_num),
            Op.hybrid_search: HybridSearchChecker(collection_name=c_name, shards_num=shards_num),
            Op.query: QueryChecker(collection_name=c_name, shards_num=shards_num),
            Op.delete: DeleteChecker(collection_name=c_name, shards_num=shards_num),
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_scale_up(self, image_tag):
        """
       steps
       """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 5, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 10})})
        # create rg
        resource_groups = []
        for i in range(2):
            name = cf.gen_unique_str("rg")
            self.utility = utility
            self.utility.create_resource_group(name, config=ResourceGroupConfig(
                requests={"node_num": 1},
                limits={"node_num": 1},
            ))
            resource_groups.append(name)
        list_all_resource_groups()
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name)
        self.health_checkers[Op.search].c_wrap.release()
        self.health_checkers[Op.search].c_wrap.load(_resource_groups=resource_groups[0:1])
        cc.start_monitor_threads(self.health_checkers)
        list_all_resource_groups()
        display_segment_distribution_info(c_name, release_name)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration//10)
            for k, v in self.health_checkers.items():
                v.check_result()
            # transfer replicas from default to another
            if i == 3:
                # transfer replicas from default rg to another rg
                list_all_resource_groups()
                display_segment_distribution_info(c_name, release_name)
                self.utility.transfer_replica(source_group=resource_groups[0], target_group=resource_groups[1],
                                              collection_name=c_name, num_replicas=1)
            list_all_resource_groups()
            display_segment_distribution_info(c_name, release_name)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_scale_down(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 8, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 6},
        ))
        # scale down rg1 from 8 to 1 node one by one
        for replicas in range(8, 1, -1):
            milvus_op.scale(release_name, 'queryNode', replicas, namespace)
            time.sleep(10)
            resource_group = self.utility.describe_resource_group(name)
            log.info(f"Resource group {name} info:\n {display_resource_group_info(resource_group)}")
            list_all_resource_groups()
        # assert the node in rg <= 1
        resource_group = self.utility.describe_resource_group(name)
        assert resource_group.num_available_node <= 1

