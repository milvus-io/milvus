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


class TestResourceGroup(TestcaseBase):

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)
        connections.disconnect("default")
        connections.remove_connection("default")

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_scale_up(self, image_tag):
        """
       steps
       """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
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
        # scale up rg1 to 8 nodes one by one
        for replicas in range(1, 8):
            milvus_op.scale(release_name, 'queryNode', replicas, namespace)
            time.sleep(10)
            # get querynode info
            qn = mil.query_nodes
            log.info(f"query node info: {len(qn)}")
            resource_group = self.utility.describe_resource_group(name)
            log.info(f"Resource group {name} info:\n {display_resource_group_info(resource_group)}")
            list_all_resource_groups()
        # assert the node in rg >= 4
        resource_group = self.utility.describe_resource_group(name)
        assert resource_group.num_available_node >= 4

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

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_all_querynode_add_into_two_different_config_rg(self, image_tag):
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
        rg_list = []
        # create rg1 with request node_num=4, limit node_num=6

        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 6},
        ))
        rg_list.append(name)
        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 3},
            limits={"node_num": 6},
        ))
        rg_list.append(name)
        # assert two rg satisfy the request node_num
        list_all_resource_groups()
        for rg in rg_list:
            resource_group = self.utility.describe_resource_group(rg)
            assert resource_group.num_available_node >= resource_group.config.requests.node_num

        # scale down rg1 from 8 to 1 node one by one
        for replicas in range(8, 1, -1):
            milvus_op.scale(release_name, 'queryNode', replicas, namespace)
            time.sleep(10)
            for name in rg_list:
                resource_group = self.utility.describe_resource_group(name)
                log.info(f"Resource group {name} info:\n {display_resource_group_info(resource_group)}")
            list_all_resource_groups()

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_querynode_add_into_two_different_config_rg_one_by_one(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        rg_list = []
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 6},
        ))
        rg_list.append(name)

        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 3},
            limits={"node_num": 6},
        ))
        rg_list.append(name)
        for replicas in range(1, 8):
            milvus_op.scale(release_name, 'queryNode', replicas, namespace)
            time.sleep(10)
            list_all_resource_groups()

        for rg in rg_list:
            resource_group = self.utility.describe_resource_group(rg)
            assert resource_group.num_available_node >= resource_group.config.requests.node_num
        # scale down rg1 from 8 to 1 node one by one
        for replicas in range(8, 1, -1):
            milvus_op.scale(release_name, 'queryNode', replicas, namespace)
            time.sleep(10)
            list_all_resource_groups()
        for rg in rg_list:
            resource_group = self.utility.describe_resource_group(rg)
            assert resource_group.num_available_node >= 1


    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_querynode_add_into_new_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)

        self.release_name = release_name
        milvus_op.scale(release_name, 'queryNode', 10, namespace)
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        rg_list = []
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 6},
        ))
        rg_list.append(name)
        for rg in rg_list:
            resource_group = self.utility.describe_resource_group(rg)
            assert resource_group.num_available_node >= resource_group.config.requests.node_num

        # create a new rg with request node_num=3, limit node_num=6
        # the querynode will be added into the new rg from default rg
        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 3},
            limits={"node_num": 6},
        ))
        rg_list.append(name)
        list_all_resource_groups()
        for rg in rg_list:
            resource_group = self.utility.describe_resource_group(rg)
            assert resource_group.num_available_node >= resource_group.config.requests.node_num

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_with_two_rg_link_to_each_other_when_all_not_reached_to_request(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        milvus_op.scale(release_name, 'queryNode', 8, namespace)
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 1})})
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        rg1_name = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 6},
        ))
        name = cf.gen_unique_str("rg")
        rg2_name = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 6},
        ))
        list_all_resource_groups()
        log.info("update resource group")
        utility.update_resource_groups(
            {rg1_name: ResourceGroupConfig(requests={"node_num": 6},
                                           limits={"node_num": 8},
                                           transfer_from=[{"resource_group": rg2_name}],
                                           transfer_to=[{"resource_group": rg2_name}], )})
        time.sleep(10)
        list_all_resource_groups()
        utility.update_resource_groups(
            {rg2_name: ResourceGroupConfig(requests={"node_num": 6},
                                           limits={"node_num": 8},
                                           transfer_from=[{"resource_group": rg1_name}],
                                           transfer_to=[{"resource_group": rg1_name}], )})
        time.sleep(10)
        list_all_resource_groups()
        # no querynode was transferred between rg1 and rg2
        resource_group = self.utility.describe_resource_group(rg1_name)
        assert resource_group.num_available_node == 4
        resource_group = self.utility.describe_resource_group(rg2_name)
        assert resource_group.num_available_node == 4

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_with_rg_transfer_from_non_default_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        milvus_op.scale(release_name, 'queryNode', 15, namespace)
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 3})})
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        rg1_name = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 2},
            limits={"node_num": 2},
        ))
        name = cf.gen_unique_str("rg")
        rg2_name = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 6},
            limits={"node_num": 10},
        ))
        list_all_resource_groups()
        rg2_available_node_before = self.utility.describe_resource_group(rg2_name).num_available_node
        log.info("update resource group")
        utility.update_resource_groups(
            {rg1_name: ResourceGroupConfig(requests={"node_num": 4},
                                           limits={"node_num": 6},
                                           transfer_from=[{"resource_group": rg2_name}],
                                           transfer_to=[{"resource_group": rg2_name}], )})
        time.sleep(10)
        list_all_resource_groups()
        # expect qn in rg 1 transfer from rg2 not the default rg
        rg2_available_node_after = self.utility.describe_resource_group(rg2_name).num_available_node
        assert rg2_available_node_before > rg2_available_node_after

    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_with_rg_transfer_to_non_default_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        milvus_op.scale(release_name, 'queryNode', 10, namespace)
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 10})})
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        rg1_name = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 2},
            limits={"node_num": 10},
        ))
        name = cf.gen_unique_str("rg")
        rg2_name = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 4},
            limits={"node_num": 4},
        ))
        list_all_resource_groups()
        rg1_node_available_before = self.utility.describe_resource_group(rg1_name).num_available_node
        log.info("update resource group")
        utility.update_resource_groups(
            {rg2_name: ResourceGroupConfig(requests={"node_num": 2},
                                           limits={"node_num": 2},
                                           transfer_from=[{"resource_group": rg1_name}],
                                           transfer_to=[{"resource_group": rg1_name}], )})
        time.sleep(10)
        list_all_resource_groups()
        # expect qn in rg 2 transfer to rg1 not the default rg
        rg1_node_available_after = self.utility.describe_resource_group(rg1_name).num_available_node
        assert rg1_node_available_after > rg1_node_available_before


    @pytest.mark.tags(CaseLabel.L3)
    def test_resource_group_with_rg_transfer_with_rg_list(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        milvus_op.scale(release_name, 'queryNode', 12, namespace)
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 1})})
        # create rg1 with request node_num=4, limit node_num=6
        name = cf.gen_unique_str("rg")
        source_rg = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 1},
            limits={"node_num": 1},
        ))
        name = cf.gen_unique_str("rg")
        small_rg = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 2},
            limits={"node_num": 4},
        ))
        name = cf.gen_unique_str("rg")
        big_rg = name
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 3},
            limits={"node_num": 6},
        ))
        list_all_resource_groups()
        small_rg_node_available_before = self.utility.describe_resource_group(small_rg).num_available_node
        big_rg_node_available_before = self.utility.describe_resource_group(big_rg).num_available_node
        log.info("update resource group")
        utility.update_resource_groups(
            {source_rg: ResourceGroupConfig(requests={"node_num": 6},
                                           limits={"node_num": 6},
                                           transfer_from=[{"resource_group": small_rg}, {"resource_group": big_rg}],
                                           )})
        time.sleep(10)
        list_all_resource_groups()
        # expect source rg transfer from small rg and big rg
        small_rg_node_available_after = self.utility.describe_resource_group(small_rg).num_available_node
        big_rg_node_available_after = self.utility.describe_resource_group(big_rg).num_available_node
        assert (small_rg_node_available_before + big_rg_node_available_before > small_rg_node_available_after +
                big_rg_node_available_after)


class TestReplicasManagement(TestcaseBase):

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)
        connections.disconnect("default")
        connections.remove_connection("default")

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replicas_one_collection_multi_replicas_to_multi_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 12, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        resource_groups = []
        for i in range(4):
            name = cf.gen_unique_str("rg")
            self.utility = utility
            self.utility.create_resource_group(name, config=ResourceGroupConfig(
                requests={"node_num": 2},
                limits={"node_num": 6},
            ))
            resource_groups.append(name)
        list_all_resource_groups()

        # create collection and load with 2 replicase
        self.skip_connection = True
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=True)[0:2]
        collection_w.release()
        log.info(f"resource groups: {resource_groups}")
        collection_w.load(replica_number=len(resource_groups), _resource_groups=resource_groups)
        list_all_resource_groups()

        # list replicas
        replicas = collection_w.get_replicas()
        log.info(f"replicas: {replicas}")
        rg_to_scale_down = resource_groups[0]
        # scale down a rg to 1 node
        self.utility.update_resource_groups(
            {rg_to_scale_down: ResourceGroupConfig(requests={"node_num": 1},
                                                   limits={"node_num": 1}, )}
        )

        list_all_resource_groups()
        replicas = collection_w.get_replicas()
        log.info(f"replicas: {replicas}")
        # scale down a rg t0 0 node
        self.utility.update_resource_groups(
            {rg_to_scale_down: ResourceGroupConfig(requests={"node_num": 0},
                                                   limits={"node_num": 0}, )}
        )
        list_all_resource_groups()
        replicas = collection_w.get_replicas()
        log.info(f"replicas: {replicas}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_multi_collection_multi_replicas_to_multi_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 12, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        # create  two rg with request node_num=4, limit node_num=6
        resource_groups = []
        for i in range(3):
            name = cf.gen_unique_str("rg")
            self.utility = utility
            self.utility.create_resource_group(name, config=ResourceGroupConfig(
                requests={"node_num": 3},
                limits={"node_num": 6},
            ))
            resource_groups.append(name)
        log.info(f"resource groups: {resource_groups}")
        list_all_resource_groups()
        col_list = []
        # create collection and load with multi replicase
        self.skip_connection = True
        for i in range(3):
            prefix = cf.gen_unique_str("test_rg")
            collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                                 enable_dynamic_field=True)[0:2]
            collection_w.release()
            col_list.append(collection_w)
            collection_w.load(replica_number=len(resource_groups), _resource_groups=resource_groups)
        list_all_resource_groups()

        # list replicas
        for col in col_list:
            replicas = col.get_replicas()
            log.info(f"replicas: {replicas}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_multi_collection_one_replicas_to_multi_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 12, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        # create  two rg with request node_num=4, limit node_num=6
        resource_groups = []
        for i in range(3):
            name = cf.gen_unique_str("rg")
            self.utility = utility
            self.utility.create_resource_group(name, config=ResourceGroupConfig(
                requests={"node_num": 3},
                limits={"node_num": 6},
            ))
            resource_groups.append(name)
        log.info(f"resource groups: {resource_groups}")
        list_all_resource_groups()
        col_list = []
        # create collection and load with multi replicase
        self.skip_connection = True
        for i in range(3):
            prefix = cf.gen_unique_str("test_rg")
            collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                                 enable_dynamic_field=True)[0:2]
            collection_w.release()
            col_list.append(collection_w)
            collection_w.load(replica_number=1, _resource_groups=resource_groups)
        list_all_resource_groups()

        # list replicas
        for col in col_list:
            replicas = col.get_replicas()
            log.info(f"replicas: {replicas}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_transfer_replicas_to_other_rg(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 12, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        # create  two rg with request node_num=4, limit node_num=6
        resource_groups = []
        for i in range(3):
            name = cf.gen_unique_str("rg")
            self.utility = utility
            self.utility.create_resource_group(name, config=ResourceGroupConfig(
                requests={"node_num": 3},
                limits={"node_num": 6},
            ))
            resource_groups.append(name)
        log.info(f"resource groups: {resource_groups}")
        list_all_resource_groups()
        col_list = []
        # create collection and load with multi replicase
        self.skip_connection = True
        for i in range(3):
            prefix = cf.gen_unique_str("test_rg")
            collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                                 enable_dynamic_field=True)[0:2]
            collection_w.release()
            col_list.append(collection_w)
            collection_w.load(replica_number=1, _resource_groups=[resource_groups[i]])
        list_all_resource_groups()
        # list replicas
        for col in col_list:
            replicas = col.get_replicas()
            log.info(f"replicas: {replicas}")

        # transfer replicas to default rg
        self.utility.transfer_replica(source_group=resource_groups[0], target_group=DEFAULT_RESOURCE_GROUP,
                                      collection_name=col_list[0].name, num_replicas=1)

        list_all_resource_groups()
        # list replicas
        for col in col_list:
            replicas = col.get_replicas()
            log.info(f"replicas: {replicas}")


class TestServiceAvailableDuringScale(TestcaseBase):

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

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)
        connections.disconnect("default")
        connections.remove_connection("default")

    def test_service_available_during_scale_up(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 3, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 10})})
        # create rg
        resource_groups = []
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
        # load collection to non default rg
        self.health_checkers[Op.search].c_wrap.release()
        self.health_checkers[Op.search].c_wrap.load(_resource_groups=resource_groups)
        cc.start_monitor_threads(self.health_checkers)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration//10)
            for k, v in self.health_checkers.items():
                v.check_result()
            # scale up querynode when progress is 3/10
            if i == 3:
                utility.update_resource_groups(
                    {name: ResourceGroupConfig(requests={"node_num": 2}, limits={"node_num": 2})})
                log.info(f"scale up querynode in rg {name} from 1 to 2")
            list_all_resource_groups()
            display_segment_distribution_info(c_name, release_name)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()

    def test_service_available_during_scale_down(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        milvus_op.scale(release_name, 'queryNode', 3, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        utility.update_resource_groups(
            {DEFAULT_RESOURCE_GROUP: ResourceGroupConfig(requests={"node_num": 0}, limits={"node_num": 5})})
        # create rg
        resource_groups = []
        name = cf.gen_unique_str("rg")
        self.utility = utility
        self.utility.create_resource_group(name, config=ResourceGroupConfig(
            requests={"node_num": 2},
            limits={"node_num": 2},
        ))
        resource_groups.append(name)
        list_all_resource_groups()
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name)
        # load collection to non default rg
        self.health_checkers[Op.search].c_wrap.release()
        self.health_checkers[Op.search].c_wrap.load(_resource_groups=resource_groups)
        cc.start_monitor_threads(self.health_checkers)
        list_all_resource_groups()
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration//10)
            for k, v in self.health_checkers.items():
                v.check_result()
            # scale down querynode in rg when progress is 3/10
            if i == 3:
                list_all_resource_groups()
                utility.update_resource_groups(
                    {name: ResourceGroupConfig(requests={"node_num": 1}, limits={"node_num": 1})})
                log.info(f"scale down querynode in rg {name} from 2 to 1")
                list_all_resource_groups()
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()


class TestServiceAvailableDuringTransferReplicas(TestcaseBase):

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

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        milvus_op = MilvusOperator()
        milvus_op.uninstall(self.release_name, namespace)
        connections.disconnect("default")
        connections.remove_connection("default")

    def test_service_available_during_transfer_replicas(self, image_tag):
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
