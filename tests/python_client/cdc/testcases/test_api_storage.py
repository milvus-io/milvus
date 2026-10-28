"""CDC contracts at object storage, external refresh and snapshot boundaries."""

import io

import pytest
from common.common_type import CaseLabel
from common.external_table_common import build_external_source, build_external_spec, get_minio_config, new_minio_client
from pymilvus import DataType, MilvusException
from pymilvus.client.types import LoadState
from pymilvus.grpc_gen import milvus_pb2

from .contract_helpers import assert_index_sync, rows_equal, rpc

pytestmark = pytest.mark.tags(CaseLabel.CDC, CaseLabel.L3)


@pytest.fixture
def storage(
    cdc_contract,
    import_2pc_minio_endpoint,
    import_2pc_minio_bucket,
    import_2pc_downstream_minio_endpoint,
    import_2pc_downstream_minio_bucket,
):
    c = cdc_contract
    configs = [
        get_minio_config(import_2pc_minio_endpoint, import_2pc_minio_bucket),
        get_minio_config(import_2pc_downstream_minio_endpoint, import_2pc_downstream_minio_bucket),
    ]
    prefix = c.name("cdc_objects")
    clients = [new_minio_client(config) for config in configs]

    def cleanup():
        for client, config in zip(clients, configs):
            for obj in client.list_objects(config["bucket"], prefix=prefix + "/", recursive=True):
                client.remove_object(config["bucket"], obj.object_name)

    c.request.addfinalizer(cleanup)
    return configs, clients, prefix


@pytest.fixture
def external_collection(cdc_contract, storage):
    """Create a replicated external schema and an uploader for primary-only refreshes."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    c = cdc_contract
    configs, stores, prefix = storage
    config, store = configs[0], stores[0]

    def upload(start):
        rows = [{"id": i, "vector": [float(i), 1.0, 0.0, 0.0], "value": i * 10} for i in range(start, start + 4)]
        table = pa.Table.from_pylist(
            rows,
            schema=pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32(), 4)), ("value", pa.int64())]),
        )
        output = io.BytesIO()
        pq.write_table(table, output)
        payload = output.getvalue()
        store.put_object(config["bucket"], f"{prefix}/data/{start}.parquet", io.BytesIO(payload), len(payload))
        return rows

    initial_rows = upload(0)
    name = c.name("external")
    c.request.addfinalizer(lambda: c.upstream.drop_collection(name))
    schema = c.upstream.create_schema(
        external_source=build_external_source(config, prefix + "/data"), external_spec=build_external_spec(config)
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4, external_field="vector")
    schema.add_field("value", DataType.INT64, external_field="value")
    c.upstream.create_collection(name, schema=schema)
    c.sync_schema(name)
    return name, upload, initial_rows


class TestCDCStorageAPI:
    @pytest.mark.parametrize("role", ["upstream", "downstream"])
    def test_file_resource_registration_and_removal_are_local(self, cdc_contract, storage, role):
        c = cdc_contract
        configs, stores, prefix = storage
        name = c.name("stopwords")
        path = prefix + "/stop_words.txt"
        for config, store in zip(configs, stores):
            payload = b"canary\n"
            store.put_object(config["bucket"], path, io.BytesIO(payload), len(payload))
        client = getattr(c, role)
        peer = c.downstream if role == "upstream" else c.upstream
        for target in (client, peer):
            c.request.addfinalizer(lambda target=target: target.remove_file_resource(name))

        def names(target):
            return {resource.name for resource in target.list_file_resources(timeout=10)}

        client.add_file_resource(name, path)
        c.control_barrier()
        assert name in names(client)
        assert name not in names(peer)
        peer.add_file_resource(name, path)
        client.remove_file_resource(name)
        c.control_barrier()
        assert name not in names(client)
        assert name in names(peer)

    def test_external_refresh_data_stays_local_and_cdc_continues(self, cdc_contract, external_collection):
        """Refresh data stays local while unrelated collection writes keep replicating."""
        c = cdc_contract
        name, upload, rows = external_collection
        canary = c.collection()
        canary_rows = []
        for iteration in range(2):
            if iteration:
                c.upstream.release_collection(name)
                rows += upload(iteration * 10)
            job = c.upstream.refresh_external_collection(name)

            def complete():
                info = c.upstream.get_refresh_external_collection_progress(job)
                assert info.state == "RefreshCompleted", info

            c.wait(complete, "external refresh completion")
            assert any(info.job_id == job for info in c.upstream.list_refresh_external_collection_jobs(name))
            if not iteration:
                index = c.upstream.prepare_index_params()
                index.add_index("vector", index_type="FLAT", metric_type="L2")
                c.upstream.create_index(name, index)
            assert_index_sync(c.upstream, c.downstream, name, "vector", c.timeout)
            c.upstream.load_collection(name)
            c.control_barrier()
            c.wait(lambda: rows_equal(c.upstream, name, rows), "primary refreshed rows")
            c.wait(
                lambda: rows_equal(c.downstream, name, [], ["id", "vector", "value"]),
                "secondary external collection remains empty",
            )
            assert not c.downstream.list_refresh_external_collection_jobs(name, timeout=10)

            # External collections reject DML, so use an ordinary collection as the control.
            new_rows = c.data(iteration * 10)
            c.upstream.insert(canary, new_rows)
            canary_rows += new_rows
            c.assert_rows(canary, canary_rows)
            rows_equal(c.upstream, name, rows)
            rows_equal(c.downstream, name, [], ["id", "vector", "value"])
            assert not c.downstream.list_refresh_external_collection_jobs(name, timeout=10)

    def test_secondary_external_refresh_rejected_without_changes(self, cdc_contract, external_collection):
        c = cdc_contract
        name, _, _ = external_collection
        index = c.upstream.prepare_index_params()
        index.add_index("vector", index_type="FLAT", metric_type="L2")
        c.upstream.create_index(name, index)
        c.upstream.load_collection(name)
        c.assert_rows(name, [], ["id", "vector", "value"])

        def loaded():
            for client in (c.upstream, c.downstream):
                assert client.get_load_state(name, timeout=10)["state"] == LoadState.Loaded

        c.wait(loaded, "both external collections loaded before the rejection baseline")

        def state(client):
            return {
                "schema": client.describe_collection(name),
                "indexes": {idx: client.describe_index(name, idx) for idx in client.list_indexes(name)},
                "partitions": client.list_partitions(name),
                "load": client.get_load_state(name),
                "rows": client.query(name, filter="id >= 0", output_fields=["id", "vector", "value"], timeout=10),
                "jobs": [(job.job_id, job.state) for job in client.list_refresh_external_collection_jobs(name)],
            }

        before = [state(client) for client in (c.upstream, c.downstream)]
        with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
            c.downstream.refresh_external_collection(name, timeout=15)
        c.control_barrier()
        assert [state(client) for client in (c.upstream, c.downstream)] == before
        canary = c.collection()
        c.upstream.insert(canary, c.data())
        c.assert_rows(canary, c.data())
        assert [state(client) for client in (c.upstream, c.downstream)] == before

    def test_snapshot_export_and_external_restore_data_stay_local(self, cdc_contract, storage):
        c = cdc_contract
        configs, _, prefix = storage
        name = c.collection()
        c.upstream.insert(name, c.data())
        c.upstream.flush(name)
        c.assert_rows(name, c.data())
        snapshot = c.name("export")
        c.request.addfinalizer(lambda: c.upstream.drop_snapshot(snapshot, name))
        c.upstream.create_snapshot(snapshot, name, timeout=c.timeout)
        exported = rpc(
            c.upstream,
            "ExportSnapshot",
            milvus_pb2.ExportSnapshotRequest(
                name=snapshot,
                collection_name=name,
                target_s3_path=f"s3://{configs[0]['bucket']}/{prefix}/export",
            ),
            timeout=c.timeout,
        )
        completed = []

        def complete():
            info = rpc(
                c.upstream, "GetExportSnapshotState", milvus_pb2.GetExportSnapshotStateRequest(job_id=exported.job_id)
            ).info
            assert milvus_pb2.ExportSnapshotState.Name(info.state) == "ExportSnapshotCompleted", info
            completed[:] = [info]

        c.wait(complete, "snapshot export")
        assert completed[0].snapshot_metadata_uri
        c.control_barrier()
        assert snapshot not in c.downstream.list_snapshots(name)
        restored = c.name("external_restore")
        c.request.addfinalizer(lambda: c.upstream.drop_collection(restored))
        response = rpc(
            c.upstream,
            "RestoreExternalSnapshot",
            milvus_pb2.RestoreExternalSnapshotRequest(
                target_collection_name=restored,
                snapshot_metadata_uri=completed[0].snapshot_metadata_uri,
            ),
            timeout=c.timeout,
        )

        def restored_complete():
            info = c.upstream.get_restore_snapshot_state(response.job_id)
            assert info.state == "RestoreSnapshotCompleted", info

        c.wait(restored_complete, "external snapshot restore")
        c.sync_schema(restored)
        assert_index_sync(c.upstream, c.downstream, restored, "vector", c.timeout)
        c.upstream.load_collection(restored)
        c.control_barrier()
        c.wait(lambda: rows_equal(c.upstream, restored, c.data()), "primary external restore rows")
        c.wait(lambda: rows_equal(c.downstream, restored, []), "secondary externally restored collection remains empty")
        assert not c.downstream.list_restore_snapshot_jobs(collection_name=restored, timeout=10)
        assert any(
            info.job_id == response.job_id
            for info in c.upstream.list_restore_snapshot_jobs(collection_name=restored, timeout=10)
        )
        new_rows = c.data(100)
        c.upstream.insert(restored, new_rows)
        c.wait(lambda: rows_equal(c.downstream, restored, new_rows), "DML after local external restore")
        rows_equal(c.upstream, restored, c.data() + new_rows)
        assert not c.downstream.list_restore_snapshot_jobs(collection_name=restored, timeout=10)
