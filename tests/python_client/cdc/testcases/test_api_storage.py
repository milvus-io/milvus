"""CDC contracts at object storage, external refresh and snapshot boundaries."""

import io

import pytest
from common.common_type import CaseLabel
from common.external_table_common import build_external_source, build_external_spec, get_minio_config, new_minio_client
from pymilvus import DataType, MilvusException
from pymilvus.grpc_gen import milvus_pb2

from .contract_helpers import rpc

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
            return {
                resource.name
                for resource in rpc(target, "ListFileResources", milvus_pb2.ListFileResourcesRequest()).resources
            }

        client.add_file_resource(name, path)
        c.control_barrier()
        assert name in names(client)
        assert name not in names(peer)
        peer.add_file_resource(name, path)
        client.remove_file_resource(name)
        c.control_barrier()
        assert name not in names(client)
        assert name in names(peer)

    def test_external_refresh_replicates_complete_business_state(self, cdc_contract, storage):
        c = cdc_contract
        configs, stores, prefix = storage
        config, store = configs[0], stores[0]
        import pyarrow as pa
        import pyarrow.parquet as pq

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

        rows = upload(0)
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
        for iteration in range(2):
            if iteration:
                c.upstream.release_collection(name)
                rows += upload(10)
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
            c.upstream.load_collection(name)
            c.assert_rows(name, rows)
        with pytest.raises(MilvusException, match="(?i)not primary|secondary"):
            c.downstream.refresh_external_collection(name, timeout=15)
        c.assert_rows(name, rows)

    def test_snapshot_export_and_external_restore_keep_replica_complete(self, cdc_contract, storage):
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
        c.upstream.load_collection(restored)
        c.assert_rows(restored, c.data())
