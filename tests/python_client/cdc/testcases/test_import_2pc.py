import pytest
from chaos.checker import Import2PCChecker
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import MilvusException, connections
from pymilvus.bulk_writer import bulk_import


class TestCDCImport2PC:
    @pytest.mark.tags(CaseLabel.CDC, CaseLabel.L3)
    @pytest.mark.parametrize("outcome", ["commit", "abort", "reject_auto_commit"])
    def test_import_2pc_manual_outcome_replicates_to_downstream(
        self,
        outcome,
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
        target: verify manual Import 2PC commit and abort propagate to both clusters
        method: create an upstream manual import job with auto_commit=false, stage the same parquet files in both
                object stores, wait for primary and secondary Uncommitted, then CommitImport on upstream
        expected: imported PKs stay invisible on both clusters before commit and become visible on both clusters
                  after commit; abort leaves both clusters unchanged and permits a new import
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
            import_timeout=max(sync_timeout, 180),
            visibility_timeout=max(sync_timeout, 180),
        )
        try:
            if outcome == "commit":
                res, ok = checker.run_task()
                assert ok, res
            else:
                rows, ids = checker._make_rows()
                result, ready = checker._wait_downstream_collection_ready()
                assert ready, result
                files = checker._write_import_files(rows)
                checker._copy_import_files_to_downstream(files)
                if outcome == "reject_auto_commit":
                    with pytest.raises(MilvusException, match="auto_commit=true.*not supported"):
                        bulk_import(
                            url=upstream_uri,
                            api_key=upstream_token,
                            collection_name=collection_name,
                            files=files,
                            options={"auto_commit": "true"},
                            timeout=60,
                        )
                    for client in (checker.milvus_client, checker.downstream_client):
                        result, absent = checker._wait_ids_absent(ids, client=client)
                        assert absent, result
                        result, unchanged = checker._wait_count(checker.expected_count, client=client)
                        assert unchanged, result
                    result, ok = checker.run_task()
                    assert ok, result
                    return
                response = bulk_import(
                    url=upstream_uri,
                    api_key=upstream_token,
                    collection_name=collection_name,
                    files=files,
                    options={"auto_commit": "false"},
                    timeout=60,
                ).json()
                assert response["code"] == 0, response
                job = checker._job_id_from_create_response(response)
                checker.pending_job_ids.add(job)
                for uri, token in ((upstream_uri, upstream_token), (downstream_uri, downstream_token)):
                    result, ready = checker._wait_import_state(job, "Uncommitted", uri=uri, token=token)
                    assert ready, result
                checker._post_import_endpoint("abort", {"jobId": job})
                for uri, token in ((upstream_uri, upstream_token), (downstream_uri, downstream_token)):
                    result, ready = checker._wait_import_state(job, "Failed", uri=uri, token=token)
                    assert ready, result
                    _, reason, _ = checker._job_state_from_progress(result)
                    assert "abort" in reason.lower(), result
                for client in (checker.milvus_client, checker.downstream_client):
                    result, absent = checker._wait_ids_absent(ids, client=client)
                    assert absent, result
                    result, unchanged = checker._wait_count(checker.expected_count, client=client)
                    assert unchanged, result
                checker._post_import_endpoint("abort", {"jobId": job})
                checker.pending_job_ids.discard(job)
                # A new successful import proves an aborted transaction does not stall CDC.
                result, ok = checker.run_task()
                assert ok, result
        finally:
            checker.terminate()
            if upstream_client.has_collection(collection_name):
                upstream_client.drop_collection(collection_name)
