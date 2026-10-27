"""Explicit unsupported-RPC and non-replicating snapshot contracts."""

import grpc
import pytest
from common.common_type import CaseLabel
from google.protobuf.message_factory import GetMessageClass
from pymilvus import MilvusException
from pymilvus.grpc_gen import milvus_pb2

from .contract_helpers import assert_index_sync, rows_equal, rpc

pytestmark = pytest.mark.tags(CaseLabel.CDC, CaseLabel.L3)


class TestCDCUnsupportedAPI:
    @pytest.mark.parametrize("role", ["upstream", "downstream"])
    @pytest.mark.parametrize(
        "method",
        [
            "Prewarm",
            "DescribePrewarmTask",
            "CreateNamespace",
            "DescribeNamespace",
            "ListNamespaces",
            "DropNamespace",
            "HasNamespace",
            "GetNamespaceStats",
        ],
    )
    def test_unimplemented_rpc_has_no_side_effects(self, cdc_contract, role, method):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        descriptor = milvus_pb2.DESCRIPTOR.services_by_name["MilvusService"].methods_by_name[method]
        request = GetMessageClass(descriptor.input_type)()
        values = {
            "db_name": "default",
            "collection_name": name,
            "namespace_name": "canary",
            "namespace": "canary",
            "taskID": "1",
        }
        for key, value in values.items():
            if key in request.DESCRIPTOR.fields_by_name:
                setattr(request, key, value)
        client = getattr(c, role)
        with c.unchanged(name):
            with pytest.raises(grpc.RpcError) as error:
                getattr(client._get_connection()._stub, method)(request, timeout=10)
            assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED
        # A transport outage must not be mistaken for an unsupported interface.
        c.upstream.insert(name, c.data(10))
        c.assert_rows(name, c.data() + c.data(10))

    def test_secondary_snapshot_creation_is_rejected(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        snapshot = c.name("snapshot_denied")
        with c.unchanged(name):
            with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
                c.downstream.create_snapshot(snapshot, name, timeout=15)
        assert snapshot not in c.downstream.list_snapshots(name)
        assert snapshot not in c.upstream.list_snapshots(name)


class TestCDCSnapshotBoundary:
    def test_snapshot_create_pin_unpin_drop_stays_local(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.upstream.flush(name)
        c.assert_rows(name, c.data())
        snapshot = c.name("snapshot_local")
        c.request.addfinalizer(lambda: c.upstream.drop_snapshot(snapshot, name))
        c.upstream.create_snapshot(snapshot, name, description="CDC local snapshot", timeout=c.timeout)
        assert snapshot in c.upstream.list_snapshots(name)
        assert c.upstream.describe_snapshot(snapshot, name).description == "CDC local snapshot"
        c.control_barrier()
        assert snapshot not in c.downstream.list_snapshots(name)
        pin = c.upstream.pin_snapshot_data(snapshot, name, ttl_seconds=600)
        try:
            with pytest.raises(MilvusException, match="(?i)pin"):
                c.upstream.drop_snapshot(snapshot, name)
            assert snapshot in c.upstream.list_snapshots(name)
        finally:
            c.upstream.unpin_snapshot_data(pin)
        c.upstream.drop_snapshot(snapshot, name)
        c.control_barrier()
        assert snapshot not in c.upstream.list_snapshots(name)
        assert snapshot not in c.downstream.list_snapshots(name)
        c.upstream.insert(name, c.data(10))
        c.assert_rows(name, c.data() + c.data(10))

    def test_restore_snapshot_data_stays_local_and_cdc_continues(self, cdc_contract):
        """Restore data stays on the primary; metadata and subsequent DML still replicate."""
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.upstream.flush(name)
        c.assert_rows(name, c.data())
        snapshot = c.name("snapshot_restore")
        c.request.addfinalizer(lambda: c.upstream.drop_snapshot(snapshot, name))
        c.upstream.create_snapshot(snapshot, name, timeout=c.timeout)
        target = c.name("restored")
        c.request.addfinalizer(lambda: c.upstream.drop_collection(target))
        job = c.upstream.restore_snapshot(snapshot, name, target, timeout=c.timeout)

        def completed():
            state = c.upstream.get_restore_snapshot_state(job)
            assert state.state == "RestoreSnapshotCompleted", state

        c.wait(completed, "source restore completion")
        assert any(info.job_id == job for info in c.upstream.list_restore_snapshot_jobs(collection_name=target))
        c.sync_schema(target)
        assert_index_sync(c.upstream, c.downstream, target, "vector", c.timeout)
        c.upstream.load_collection(target)
        c.control_barrier()
        c.wait(lambda: rows_equal(c.upstream, target, c.data()), "primary restored rows")
        c.wait(lambda: rows_equal(c.downstream, target, []), "secondary restored collection remains empty")
        assert not c.downstream.list_restore_snapshot_jobs(collection_name=target, timeout=10)

        # Observe subsequent DML on the same collection before checking absence again.
        new_rows = c.data(100)
        c.upstream.insert(target, new_rows)
        c.wait(lambda: rows_equal(c.downstream, target, new_rows), "DML after local snapshot restore")
        rows_equal(c.upstream, target, c.data() + new_rows)
        assert not c.downstream.list_restore_snapshot_jobs(collection_name=target, timeout=10)

    def test_manifest_secondary_rejected_without_data_changes(self, cdc_contract):
        c = cdc_contract
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.assert_rows(name, c.data())
        # The valid collection reaches the primary-only broadcast entrance before item apply.
        request = milvus_pb2.BatchUpdateManifestRequest(
            collection_name=name, items=[milvus_pb2.BatchUpdateManifestItem(segment_id=1, manifest_version=1)]
        )
        with c.unchanged(name):
            with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
                rpc(c.downstream, "BatchUpdateManifest", request)
