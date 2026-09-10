import pytest
from chaos.checker import Import2PCChecker
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import connections


class TestCDCImport2PC:
    @pytest.mark.tags(CaseLabel.CDC)
    def test_import_2pc_manual_commit_replicates_to_downstream(
        self,
        upstream_client,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        import_2pc_minio_endpoint,
        import_2pc_minio_bucket,
        import_2pc_downstream_minio_endpoint,
        import_2pc_downstream_minio_bucket,
        import_2pc_rows,
        sync_timeout,
    ):
        """
        target: verify Import 2PC manual commit is a valid CDC workload
        method: create an upstream manual import job with auto_commit=false, stage the same parquet files in both
                object stores, wait for primary and secondary Uncommitted, then CommitImport on upstream
        expected: imported PKs stay invisible on both clusters before commit and become visible on both clusters
                  after the replicated CommitImport is consumed
        """
        collection_name = cf.gen_unique_str("CDCImport2PC_")
        connections.connect("default", uri=upstream_uri, token=upstream_token)
        checker = Import2PCChecker(
            collection_name=collection_name,
            rows_per_import=import_2pc_rows,
            minio_endpoint=import_2pc_minio_endpoint,
            bucket_name=import_2pc_minio_bucket,
            uri=upstream_uri,
            token=upstream_token,
            downstream_uri=downstream_uri,
            downstream_token=downstream_token,
            downstream_minio_endpoint=import_2pc_downstream_minio_endpoint,
            downstream_bucket_name=import_2pc_downstream_minio_bucket,
            visibility_timeout=max(sync_timeout, 180),
        )
        try:
            res, ok = checker.run_task()
            assert ok, res
        finally:
            checker.terminate()
            if upstream_client.has_collection(collection_name):
                upstream_client.drop_collection(collection_name)
