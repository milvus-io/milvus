"""
CDC test for idempotent Insert retries across a planned switchover.

An Insert that carries an Idempotency-Key is replicated to the secondary with the
primary keys the primary assigned. The secondary records the key, so after a
planned switchover a retry of the same key on the promoted cluster returns those
primary keys and adds no rows (milvus-io/milvus#54572).
"""

import time
import uuid

import requests

from .base import TestCDCSyncBase, logger

DIM = 8
ROWS = 32


def rest_insert(uri: str, token: str, collection_name: str, key: str, rows: list[dict]) -> list[int]:
    """Insert through REST v2 with an Idempotency-Key header and return the insert IDs.

    REST is used because the header is the one client entry point that every SDK
    version forwards unchanged.
    """
    response = requests.post(
        uri.rstrip("/") + "/v2/vectordb/entities/insert",
        headers={
            "Authorization": "Bearer " + token,
            "Idempotency-Key": key,
            "Accept-Type-Allow-Int64": "true",
        },
        json={"collectionName": collection_name, "data": rows},
        timeout=60,
    )
    response.raise_for_status()
    body = response.json()
    assert body["code"] == 0, body
    assert body["data"]["insertCount"] == len(rows), body
    return [int(i) for i in body["data"]["insertIds"]]


class TestCDCIdempotentInsertSwitchover(TestCDCSyncBase):
    """Idempotent Insert retries must stay deduplicated across a planned switchover."""

    def setup_method(self):
        self.resources_to_cleanup = []

    def teardown_method(self):
        upstream_client = getattr(self, "_upstream_client", None)
        if upstream_client:
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "collection":
                    self.cleanup_collection(upstream_client, resource_name)
            time.sleep(1)  # Allow cleanup to sync to downstream

    def test_idempotent_retry_after_switchover(
        self,
        upstream_client,
        downstream_client,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Insert 32 autoID rows on the primary with key K and retry K there (served from
        the window). Wait until the secondary holds the same 32 IDs. Switch over, then
        retry K on the promoted cluster: it must return the original IDs, and both
        count(*) and the physical row count must stay 32.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_idem", max_length=50)

        self.log_test_start("test_idempotent_retry_after_switchover", "SWITCHOVER_IDEMPOTENT_RETRY", c_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))
        switched = False

        try:
            self.cleanup_collection(upstream_client, c_name)
            upstream_client.create_collection(
                collection_name=c_name,
                dimension=DIM,
                auto_id=True,
                num_shards=1,
                consistency_level="Strong",
            )

            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(check_create, sync_timeout, f"create collection {c_name}")

            key = "retry-" + uuid.uuid4().hex
            rows = [{"vector": [float((i + j) % 17) / 17 for j in range(DIM)]} for i in range(ROWS)]
            original = rest_insert(upstream_uri, upstream_token, c_name, key, rows)
            assert len(original) == ROWS

            # Sanity: a retry on the primary is answered from its own window.
            assert rest_insert(upstream_uri, upstream_token, c_name, key, rows) == original

            def check_replicated():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["id"],
                        limit=ROWS * 2,
                        consistency_level="Strong",
                    )
                    ids = sorted(int(r["id"]) for r in res)
                    logger.info(f"[SYNC_PROGRESS] downstream rows: {len(ids)}/{ROWS}")
                    return ids == sorted(original)
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(check_replicated, sync_timeout, f"replicate {ROWS} rows of {c_name}")

            logger.info("[SWITCHOVER] Promoting the downstream cluster...")
            switchover_helper(target_cluster_id, source_cluster_id)
            switched = True

            retried = rest_insert(downstream_uri, downstream_token, c_name, key, rows)
            assert retried == original, (
                f"retry after switchover returned different IDs: original={original[:3]}..., retried={retried[:3]}..."
            )

            count = downstream_client.query(
                collection_name=c_name,
                filter="",
                output_fields=["count(*)"],
                consistency_level="Strong",
            )[0]["count(*)"]
            assert count == ROWS, f"count(*) after switchover retry: {count}, expected {ROWS}"

            # A duplicate with identical primary keys is invisible to a query, so
            # check the physical row count too.
            downstream_client.flush(c_name)
            physical = int(downstream_client.get_collection_stats(c_name)["row_count"])
            assert physical == ROWS, f"physical rows after switchover retry: {physical}, expected {ROWS}"

        finally:
            if switched:
                logger.info("[SWITCHOVER] Restoring original topology...")
                switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end("test_idempotent_retry_after_switchover", True, time.time() - start_time)
