import pytest
import time
from pymilvus import connections, utility, Collection
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
from chaos.checker import (InsertChecker,
                           FlushChecker,
                           UpsertChecker,
                           DeleteChecker,
                           Op,
                           ResultAnalyzer
                           )
from chaos import chaos_commons as cc
from common import common_func as cf
from utils.util_k8s import get_querynode_id_pod_pairs
from utils.util_birdwatcher import BirdWatcher
from customize.milvus_operator import MilvusOperator
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel
from chaos.chaos_commons import assert_statistic

namespace = 'chaos-testing'
prefix = "test_rg"

from rich.table import Table
from rich.console import Console


def display_segment_distribution_info(collection_name, release_name, segment_info=None):
    table = Table(title=f"{collection_name} Segment Distribution Info")
    table.width = 200
    table.add_column("Segment ID", style="cyan")
    table.add_column("Collection ID", style="cyan")
    table.add_column("Partition ID", style="cyan")
    table.add_column("Num Rows", style="cyan")
    table.add_column("State", style="cyan")
    table.add_column("Channel", style="cyan")
    table.add_column("Node ID", style="cyan")
    table.add_column("Node Name", style="cyan")
    res = utility.get_query_segment_info(collection_name)
    log.info(f"segment info: {res}")
    label = f"app.kubernetes.io/instance={release_name}, app.kubernetes.io/component=querynode"
    querynode_id_pod_pair = get_querynode_id_pod_pairs("chaos-testing", label)
    for r in res:
        channel = "unknown"
        if segment_info and str(r.segmentID) in segment_info:
            channel = segment_info[str(r.segmentID)]["Insert Channel"]
        table.add_row(
            str(r.segmentID),
            str(r.collectionID),
            str(r.partitionID),
            str(r.num_rows),
            str(r.state),
            str(channel),
            str(r.nodeIds),
            str([querynode_id_pod_pair.get(node_id) for node_id in r.nodeIds])
        )
    console = Console()
    console.width = 300
    console.print(table)


def display_channel_on_qn_distribution_info(collection_name, release_name, segment_info=None):
    """
    node id, node name, channel, segment id
    1, rg-test-613938-querynode-0, [rg-test-613938-rootcoord-dml_3_449617770820133536v0], [449617770820133655]
    2, rg-test-613938-querynode-1, [rg-test-613938-rootcoord-dml_3_449617770820133537v0], [449617770820133656]

    """
    m = {}
    res = utility.get_query_segment_info(collection_name)
    for r in res:
        if r.nodeIds:
            for node_id in r.nodeIds:
                if node_id not in m:
                    m[node_id] = {
                        "node_name": "",
                        "channel": [],
                        "segment_id": []
                    }
                m[node_id]["segment_id"].append(r.segmentID)
    # get channel info
    for node_id in m.keys():
        for seg in m[node_id]["segment_id"]:
            if segment_info and str(seg) in segment_info:
                m[node_id]["channel"].append(segment_info[str(seg)]["Insert Channel"])

    # get node name
    label = f"app.kubernetes.io/instance={release_name}, app.kubernetes.io/component=querynode"
    querynode_id_pod_pair = get_querynode_id_pod_pairs("chaos-testing", label)
    for node_id in m.keys():
        m[node_id]["node_name"] = querynode_id_pod_pair.get(node_id)

    table = Table(title=f"{collection_name} Channel Distribution Info")
    table.width = 200
    table.add_column("Node ID", style="cyan")
    table.add_column("Node Name", style="cyan")
    table.add_column("Channel", style="cyan")
    table.add_column("Segment ID", style="cyan")
    for node_id, v in m.items():
        table.add_row(
            str(node_id),
            str(v["node_name"]),
            "\n".join([str(x) for x in set(v["channel"])]),
            "\n".join([str(x) for x in v["segment_id"]])
        )
    console = Console()
    console.width = 300
    console.print(table)
    return m


def _install_milvus(image_tag="master-latest"):
    release_name = f"rg-test-{cf.gen_digits_by_length(6)}"
    cus_configs = {'spec.mode': 'cluster',
                   'spec.dependencies.msgStreamType': 'kafka',
                   'spec.components.image': f'harbor.milvus.io/milvus/milvus:{image_tag}',
                   'metadata.namespace': namespace,
                   'metadata.name': release_name,
                   'spec.components.proxy.serviceType': 'LoadBalancer',
                   'spec.config.queryCoord.balancer': 'ChannelLevelScoreBalancer',
                   'spec.config.queryCoord.channelExclusiveNodeFactor': 2
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

    def init_health_checkers(self, collection_name=None, shards_num=2):
        c_name = collection_name
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name, shards_num=shards_num),
            Op.flush: FlushChecker(collection_name=c_name, shards_num=shards_num),
            Op.upsert: UpsertChecker(collection_name=c_name, shards_num=shards_num),
            Op.delete: DeleteChecker(collection_name=c_name, shards_num=shards_num),
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_channel_exclusive_balance_during_qn_scale_up(self, image_tag):
        """
       steps
       """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        qn_num = 1
        milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        etcd_endpoint = milvus_op.etcd_endpoints(release_name, namespace)
        bw = BirdWatcher(etcd_endpoints=etcd_endpoint, root_path=release_name)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name)
        c = Collection(name=c_name)
        res = c.describe()
        collection_id = res["collection_id"]
        cc.start_monitor_threads(self.health_checkers)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration // 10)
            for k, v in self.health_checkers.items():
                v.check_result()
            qn_num += min(qn_num + 1, 8)
            seg_res = bw.show_segment_info(collection_id)
            display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
            display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        milvus_op.scale(release_name, 'queryNode', 8, namespace)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()
            time.sleep(60)
        # in final state, channel exclusive balance is on, so all qn should have only one channel
        for k, v in res.items():
            assert len(set(v["channel"])) == 1


    @pytest.mark.tags(CaseLabel.L3)
    def test_channel_exclusive_balance_during_qn_scale_down(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        qn_num = 8
        milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        etcd_endpoint = milvus_op.etcd_endpoints(release_name, namespace)
        bw = BirdWatcher(etcd_endpoints=etcd_endpoint, root_path=release_name)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name)
        c = Collection(name=c_name)
        res = c.describe()
        collection_id = res["collection_id"]
        cc.start_monitor_threads(self.health_checkers)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration // 10)
            for k, v in self.health_checkers.items():
                v.check_result()
            qn_num = max(qn_num - 1, 3)
            milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
            seg_res = bw.show_segment_info(collection_id)
            display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
            display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        milvus_op.scale(release_name, 'queryNode', 1, namespace)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()
            time.sleep(60)
        # shard num = 2, k = 2, qn_num = 3
        # in final state, channel exclusive balance is off, so all qn should have more than one channel
        for k, v in res.items():
            assert len(set(v["channel"])) > 1

    @pytest.mark.tags(CaseLabel.L3)
    def test_channel_exclusive_balance_with_channel_num_is_1(self, image_tag):
        """
       steps
       """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        qn_num = 1
        milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        etcd_endpoint = milvus_op.etcd_endpoints(release_name, namespace)
        bw = BirdWatcher(etcd_endpoints=etcd_endpoint, root_path=release_name)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name,  shards_num=1)
        c = Collection(name=c_name)
        res = c.describe()
        collection_id = res["collection_id"]
        cc.start_monitor_threads(self.health_checkers)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration // 10)
            for k, v in self.health_checkers.items():
                v.check_result()
            qn_num = qn_num + 1
            qn_num = min(qn_num, 8)
            milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
            seg_res = bw.show_segment_info(collection_id)
            display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
            res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
            for r in res:
                assert len(set(r["channel"])) == 1
        milvus_op.scale(release_name, 'queryNode', 8, namespace)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()
            time.sleep(60)

        # since shard num is 1, so all qn should have only one channel, no matter what k is
        for k, v in res.items():
            assert len(set(v["channel"])) == 1

    @pytest.mark.tags(CaseLabel.L3)
    def test_channel_exclusive_balance_after_k_increase(self, image_tag):
        """
        steps
        """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        qn_num = 1
        milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        etcd_endpoint = milvus_op.etcd_endpoints(release_name, namespace)
        bw = BirdWatcher(etcd_endpoints=etcd_endpoint, root_path=release_name)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name)
        c = Collection(name=c_name)
        res = c.describe()
        collection_id = res["collection_id"]
        cc.start_monitor_threads(self.health_checkers)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration // 10)
            for k, v in self.health_checkers.items():
                v.check_result()
            qn_num = qn_num + 1
            qn_num = min(qn_num, 8)
            if qn_num == 5:
                config = {
                    "spec.config.queryCoord.channelExclusiveNodeFactor": 3
                }
                milvus_op.upgrade(release_name, config, namespace)
            milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
            seg_res = bw.show_segment_info(collection_id)
            display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
            res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
            if qn_num == 4:
                # channel exclusive balance is on, so all qn should have only one channel
                for r in res.values():
                    assert len(set(r["channel"])) == 1
            if qn_num == 5:
                # k is changed to 3 when qn_num is 5,
                # channel exclusive balance is off, so all qn should have more than one channel
                # wait for a while to make sure all qn have more than one channel
                ready = False
                t0 = time.time()
                while not ready and time.time() - t0 < 180:
                    ready = True
                    for r in res.values():
                        if len(set(r["channel"])) == 1:
                            ready = False
                    time.sleep(10)
                    res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
            if qn_num == 6:
                # channel exclusive balance is on, so all qn should have only one channel
                ready = False
                t0 = time.time()
                while not ready and time.time() - t0 < 180:
                    ready = True
                    for r in res.values():
                        if len(set(r["channel"])) != 1:
                            ready = False
                    time.sleep(10)
                    res = display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        milvus_op.scale(release_name, 'queryNode', 8, namespace)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()
            time.sleep(60)

    @pytest.mark.tags(CaseLabel.L3)
    def test_channel_exclusive_balance_for_search_performance(self, image_tag):
        """
       steps
       """
        milvus_op = MilvusOperator()
        release_name, host, port = _install_milvus(image_tag=image_tag)
        qn_num = 1
        milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
        self.release_name = release_name
        assert host is not None
        connections.connect("default", host=host, port=port)
        etcd_endpoint = milvus_op.etcd_endpoints(release_name, namespace)
        bw = BirdWatcher(etcd_endpoints=etcd_endpoint, root_path=release_name)
        mil = MilvusSys(alias="default")
        log.info(f"milvus build version: {mil.build_version}")
        c_name = cf.gen_unique_str("Checker_")
        self.init_health_checkers(collection_name=c_name)
        c = Collection(name=c_name)
        res = c.describe()
        collection_id = res["collection_id"]
        cc.start_monitor_threads(self.health_checkers)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        log.info("*********************Load Start**********************")
        request_duration = 360
        for i in range(10):
            time.sleep(request_duration // 10)
            for k, v in self.health_checkers.items():
                v.check_result()
            qn_num = qn_num + 1
            qn_num = min(qn_num, 8)
            milvus_op.scale(release_name, 'queryNode', qn_num, namespace)
            seg_res = bw.show_segment_info(collection_id)
            display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
            display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        milvus_op.scale(release_name, 'queryNode', 8, namespace)
        seg_res = bw.show_segment_info(collection_id)
        display_segment_distribution_info(c_name, release_name, segment_info=seg_res)
        display_channel_on_qn_distribution_info(c_name, release_name, segment_info=seg_res)
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        assert_statistic(self.health_checkers)
        for k, v in self.health_checkers.items():
            v.terminate()
            time.sleep(60)
