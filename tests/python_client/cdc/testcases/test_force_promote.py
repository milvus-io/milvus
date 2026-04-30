"""
CDC force-promote failover scenario tests.

Each test exercises a path through the streamingnode replicate interceptor's
SwitchReplicateMode + force-promote ack-callback flow:
- basic           — kill source, promote secondary, verify writable
- target_restart  — restart secondary mid-promote
- source_restart  — restart primary mid-promote
- incomplete_ddl  — release on primary then immediately promote (Ignore=true path)
- network_partition — partition source↔target, then promote
- endurance (skipif) — repeated promote/restore loop for ENDURANCE_DURATION_MINUTES

Every test's `finally` restores the A→B topology via switchover_helper so
subsequent tests start from a known state.
"""

import os
import subprocess
import tempfile
import threading
import time

import pytest
import yaml
from common.common_type import CaseLabel

from cdc.conftest import CDC_UPDATE_REPLICATE_TIMEOUT_SECONDS

from .base import TestCDCSyncBase, logger

FAILOVER_TIMEOUT = int(os.getenv("FAILOVER_TIMEOUT", "180"))
PROMOTE_RETRY_INTERVAL = 5
DEFAULT_INSERT_COUNT = 200


@pytest.mark.tags(CaseLabel.CDC)
class TestCDCForcePromote(TestCDCSyncBase):
    """Force-promote failover scenarios."""

    def setup_method(self):
        self.resources_to_cleanup = []
        self._upstream_client = None
        self._downstream_client = None

    def teardown_method(self):
        """Safety-net cleanup: iterates resources_to_cleanup if a test raised
        before its own finally could run. Normal cleanup happens in each test's
        finally block."""
        for resource_type, resource_name in getattr(self, "resources_to_cleanup", []):
            if resource_type != "collection":
                continue
            for client in (
                getattr(self, "_upstream_client", None),
                getattr(self, "_downstream_client", None),
            ):
                if client is None:
                    continue
                try:
                    self.cleanup_collection(client, resource_name)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def do_force_promote(
        self,
        downstream_client,
        target_cluster_id,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """Call force_promote on downstream with a standalone-primary config."""
        config = {
            "clusters": [
                {
                    "cluster_id": target_cluster_id,
                    "connection_param": {"uri": downstream_uri, "token": downstream_token},
                    "pchannels": [f"{target_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                }
            ],
            "cross_cluster_topology": [],
        }
        downstream_client.update_replicate_configuration(
            timeout=CDC_UPDATE_REPLICATE_TIMEOUT_SECONDS,
            force_promote=True,
            **config,
        )
        logger.info(f"[FORCE_PROMOTE] called on {target_cluster_id} (standalone primary config)")

    def is_already_primary_error(self, err):
        return "force promote can only be used on secondary clusters" in str(
            err
        ) and "current cluster is primary" in str(err)

    def promote_call_until_writable(
        self,
        downstream_client,
        target_cluster_id,
        downstream_uri,
        downstream_token,
        pchannel_num,
        timeout=FAILOVER_TIMEOUT,
    ):
        """Retry force_promote + probe write until success or timeout.

        A probe write creates+drops a throwaway collection. This proves
        downstream is in standalone-primary mode and accepts DDL.
        """
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                try:
                    self.do_force_promote(
                        downstream_client,
                        target_cluster_id,
                        downstream_uri,
                        downstream_token,
                        pchannel_num,
                    )
                except Exception as e:
                    if not self.is_already_primary_error(e):
                        raise
                    logger.info("[FORCE_PROMOTE] target is already primary; probing writable state")
                probe = self.gen_unique_name("promote_probe", max_length=40)
                schema = self.create_default_schema(downstream_client)
                downstream_client.create_collection(probe, schema=schema)
                downstream_client.drop_collection(probe)
                logger.info("[FORCE_PROMOTE] probe write succeeded — cluster writable")
                return
            except Exception as e:
                last_err = e
                logger.warning(f"[FORCE_PROMOTE_RETRY] failed: {e}")
                time.sleep(PROMOTE_RETRY_INTERVAL)
        raise TimeoutError(f"force_promote did not become writable within {timeout}s; last error: {last_err}")

    def cleanup_resources(self):
        """Drop any collections registered in resources_to_cleanup."""
        for resource_type, resource_name in self.resources_to_cleanup:
            if resource_type != "collection":
                continue
            for client_label, client in (
                ("upstream", self._upstream_client),
                ("downstream", self._downstream_client),
            ):
                if client is None:
                    continue
                try:
                    self.cleanup_collection(client, resource_name)
                    logger.info(f"[CLEANUP] dropped {resource_name} on {client_label}")
                except Exception as e:
                    logger.warning(f"[CLEANUP] failed to drop {resource_name} on {client_label}: {e}")
        self.resources_to_cleanup = []

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_force_promote_basic(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        kubectl_helper,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """Insert N → kill source → force_promote(downstream) → verify writable."""
        start_time = time.time()
        c_name = self.gen_unique_name("test_fp_basic", max_length=50)
        c_after = f"{c_name}_after"

        self.log_test_start("test_force_promote_basic", "FORCE_PROMOTE_BASIC", c_name)
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client
        self.resources_to_cleanup.append(("collection", c_name))
        self.resources_to_cleanup.append(("collection", c_after))

        try:
            self.cleanup_collection(upstream_client, c_name)
            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)

            assert self.wait_for_sync(
                lambda: downstream_client.has_collection(c_name),
                sync_timeout,
                f"create collection {c_name}",
            )

            data = self.generate_test_data_with_id(DEFAULT_INSERT_COUNT, start_id=0)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_synced():
                try:
                    res = downstream_client.query(collection_name=c_name, filter="", output_fields=["count(*)"])
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {cnt}/{DEFAULT_INSERT_COUNT}")
                    return cnt >= DEFAULT_INSERT_COUNT
                except Exception as e:
                    logger.warning(f"sync check failed: {e}")
                    return False

            assert self.wait_for_sync(check_synced, sync_timeout, f"initial sync {c_name}")

            # Kill source
            logger.info(f"[FAILOVER] Killing source pods (instance={source_cluster_id})...")
            kubectl_helper.delete_pods(source_cluster_id)

            # Promote downstream — retry until writable
            self.promote_call_until_writable(
                downstream_client,
                target_cluster_id,
                downstream_uri,
                downstream_token,
                pchannel_num,
            )

            # On downstream (now standalone primary), create a new collection + insert
            schema_after = self.create_manual_id_schema(downstream_client)
            downstream_client.create_collection(collection_name=c_after, schema=schema_after)
            idx_after = downstream_client.prepare_index_params()
            idx_after.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            downstream_client.create_index(c_after, idx_after)
            downstream_client.load_collection(c_after)
            data_after = self.generate_test_data_with_id(100, start_id=0)
            downstream_client.insert(c_after, data_after)
            downstream_client.flush(c_after)

            res = downstream_client.query(collection_name=c_after, filter="", output_fields=["count(*)"])
            assert res and res[0]["count(*)"] >= 100, f"downstream not writable after force_promote: count={res}"
        finally:
            logger.info("[TEARDOWN] Waiting for source pods before topology restore...")
            kubectl_helper.wait_for_pods_ready(source_cluster_id, timeout=300)
            logger.info("[TEARDOWN] Restoring A→B topology...")
            try:
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as e:
                logger.warning(f"switchover restore failed: {e}")
            self.cleanup_resources()
            self.log_test_end("test_force_promote_basic", True, time.time() - start_time)

    def test_force_promote_during_target_restart(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        kubectl_helper,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """Promote downstream while downstream pods are bouncing.

        Mirrors snippets test_restart_b_during_force_promote.py: promote runs
        async, restart happens in main thread mid-promote.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_fp_target_restart", max_length=50)
        c_after = f"{c_name}_after"

        self.log_test_start(
            "test_force_promote_during_target_restart",
            "FORCE_PROMOTE_TARGET_RESTART",
            c_name,
        )
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client
        self.resources_to_cleanup.append(("collection", c_name))
        self.resources_to_cleanup.append(("collection", c_after))

        try:
            self.cleanup_collection(upstream_client, c_name)
            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)
            assert self.wait_for_sync(
                lambda: downstream_client.has_collection(c_name),
                sync_timeout,
                f"create collection {c_name}",
            )

            data = self.generate_test_data_with_id(DEFAULT_INSERT_COUNT, start_id=0)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_synced():
                try:
                    res = downstream_client.query(collection_name=c_name, filter="", output_fields=["count(*)"])
                    return (res[0]["count(*)"] if res else 0) >= DEFAULT_INSERT_COUNT
                except Exception:
                    return False

            assert self.wait_for_sync(check_synced, sync_timeout, f"sync {c_name}")

            # Promote async — first call almost certainly fails because target dies
            promote_err = []

            def promote_thread():
                try:
                    self.do_force_promote(
                        downstream_client,
                        target_cluster_id,
                        downstream_uri,
                        downstream_token,
                        pchannel_num,
                    )
                except Exception as e:
                    promote_err.append(e)

            th = threading.Thread(target=promote_thread, daemon=True)
            th.start()

            # Mid-promote: kill target pods, wait for them to come back
            time.sleep(2)
            logger.info("[FAILOVER] Killing target pods mid-promote...")
            kubectl_helper.delete_pods(target_cluster_id)
            time.sleep(2)
            kubectl_helper.wait_for_pods_ready(target_cluster_id, timeout=300)

            th.join(timeout=FAILOVER_TIMEOUT)
            if promote_err:
                logger.info(f"[EXPECTED] async promote raised: {promote_err[0]}")

            # Retry until writable
            self.promote_call_until_writable(
                downstream_client,
                target_cluster_id,
                downstream_uri,
                downstream_token,
                pchannel_num,
            )

            schema_after = self.create_manual_id_schema(downstream_client)
            downstream_client.create_collection(collection_name=c_after, schema=schema_after)
            idx_after = downstream_client.prepare_index_params()
            idx_after.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            downstream_client.create_index(c_after, idx_after)
            downstream_client.load_collection(c_after)
            data_after = self.generate_test_data_with_id(100, start_id=0)
            downstream_client.insert(c_after, data_after)
            downstream_client.flush(c_after)
            res = downstream_client.query(collection_name=c_after, filter="", output_fields=["count(*)"])
            assert res and res[0]["count(*)"] >= 100, f"downstream not writable after target restart: {res}"
        finally:
            logger.info("[TEARDOWN] Restoring A→B topology...")
            try:
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as e:
                logger.warning(f"switchover restore failed: {e}")
            self.cleanup_resources()
            self.log_test_end(
                "test_force_promote_during_target_restart",
                True,
                time.time() - start_time,
            )

    def test_force_promote_during_source_restart(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        kubectl_helper,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """Promote downstream while upstream pods are bouncing.

        Mirrors snippets test_restart_a_during_force_promote.py.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_fp_source_restart", max_length=50)
        c_after = f"{c_name}_after"

        self.log_test_start(
            "test_force_promote_during_source_restart",
            "FORCE_PROMOTE_SOURCE_RESTART",
            c_name,
        )
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client
        self.resources_to_cleanup.append(("collection", c_name))
        self.resources_to_cleanup.append(("collection", c_after))

        try:
            self.cleanup_collection(upstream_client, c_name)
            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)
            assert self.wait_for_sync(
                lambda: downstream_client.has_collection(c_name),
                sync_timeout,
                f"create collection {c_name}",
            )

            data = self.generate_test_data_with_id(DEFAULT_INSERT_COUNT, start_id=0)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_synced():
                try:
                    res = downstream_client.query(collection_name=c_name, filter="", output_fields=["count(*)"])
                    return (res[0]["count(*)"] if res else 0) >= DEFAULT_INSERT_COUNT
                except Exception:
                    return False

            assert self.wait_for_sync(check_synced, sync_timeout, f"sync {c_name}")

            promote_err = []

            def promote_thread():
                try:
                    self.do_force_promote(
                        downstream_client,
                        target_cluster_id,
                        downstream_uri,
                        downstream_token,
                        pchannel_num,
                    )
                except Exception as e:
                    promote_err.append(e)

            th = threading.Thread(target=promote_thread, daemon=True)
            th.start()

            time.sleep(2)
            logger.info("[FAILOVER] Killing source pods mid-promote...")
            kubectl_helper.delete_pods(source_cluster_id)
            # Don't wait-ready here — promotion should succeed independently
            # of whether the old primary is back.

            th.join(timeout=FAILOVER_TIMEOUT)
            if promote_err:
                logger.info(f"[INFO] async promote raised (may be expected): {promote_err[0]}")

            self.promote_call_until_writable(
                downstream_client,
                target_cluster_id,
                downstream_uri,
                downstream_token,
                pchannel_num,
            )

            schema_after = self.create_manual_id_schema(downstream_client)
            downstream_client.create_collection(collection_name=c_after, schema=schema_after)
            idx_after = downstream_client.prepare_index_params()
            idx_after.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            downstream_client.create_index(c_after, idx_after)
            downstream_client.load_collection(c_after)
            data_after = self.generate_test_data_with_id(100, start_id=0)
            downstream_client.insert(c_after, data_after)
            downstream_client.flush(c_after)
            res = downstream_client.query(collection_name=c_after, filter="", output_fields=["count(*)"])
            assert res and res[0]["count(*)"] >= 100, f"downstream not writable after source restart: {res}"
        finally:
            logger.info("[TEARDOWN] Waiting for source pods before topology restore...")
            kubectl_helper.wait_for_pods_ready(source_cluster_id, timeout=300)
            logger.info("[TEARDOWN] Restoring A→B topology...")
            try:
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as e:
                logger.warning(f"switchover restore failed: {e}")
            self.cleanup_resources()
            self.log_test_end(
                "test_force_promote_during_source_restart",
                True,
                time.time() - start_time,
            )

    def test_force_promote_with_incomplete_ddl(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """release_collection on upstream then immediately force_promote.

        Exercises the Ignore=true path in
        ackCallbackScheduler.fixIncompleteBroadcastsForForcePromote — DDL that
        was mid-broadcast when the operator promoted gets marked Ignore=true
        and resubmitted with replicate header cleared.

        Mirrors snippets test_incomplete_broadcast_ddl.py.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_fp_inc_ddl", max_length=50)
        c_after = f"{c_name}_after"

        self.log_test_start(
            "test_force_promote_with_incomplete_ddl",
            "FORCE_PROMOTE_INCOMPLETE_DDL",
            c_name,
        )
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client
        self.resources_to_cleanup.append(("collection", c_name))
        self.resources_to_cleanup.append(("collection", c_after))

        try:
            self.cleanup_collection(upstream_client, c_name)
            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)
            assert self.wait_for_sync(
                lambda: downstream_client.has_collection(c_name),
                sync_timeout,
                f"create collection {c_name}",
            )

            data = self.generate_test_data_with_id(DEFAULT_INSERT_COUNT, start_id=0)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_synced():
                try:
                    res = downstream_client.query(collection_name=c_name, filter="", output_fields=["count(*)"])
                    return (res[0]["count(*)"] if res else 0) >= DEFAULT_INSERT_COUNT
                except Exception:
                    return False

            assert self.wait_for_sync(check_synced, sync_timeout, f"sync {c_name}")

            # In-flight DDL on upstream — may not finish replicating before promote
            upstream_client.release_collection(c_name)
            logger.info(f"[INCOMPLETE_DDL] release_collection sent on {c_name}")

            # Immediately promote downstream (no sleep)
            self.promote_call_until_writable(
                downstream_client,
                target_cluster_id,
                downstream_uri,
                downstream_token,
                pchannel_num,
            )

            # On downstream (now primary), create new collection + insert
            schema_after = self.create_manual_id_schema(downstream_client)
            downstream_client.create_collection(collection_name=c_after, schema=schema_after)
            idx_after = downstream_client.prepare_index_params()
            idx_after.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            downstream_client.create_index(c_after, idx_after)
            downstream_client.load_collection(c_after)
            data_after = self.generate_test_data_with_id(100, start_id=0)
            downstream_client.insert(c_after, data_after)
            downstream_client.flush(c_after)
            res = downstream_client.query(collection_name=c_after, filter="", output_fields=["count(*)"])
            assert res and res[0]["count(*)"] >= 100, f"downstream not writable after incomplete DDL: {res}"
        finally:
            logger.info("[TEARDOWN] Restoring A→B topology...")
            try:
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as e:
                logger.warning(f"switchover restore failed: {e}")
            self.cleanup_resources()
            self.log_test_end(
                "test_force_promote_with_incomplete_ddl",
                True,
                time.time() - start_time,
            )

    def _build_partition_manifest(self, source_cluster_id, target_cluster_id, milvus_ns):
        """Return a NetworkChaos dict that bidirectionally partitions source ↔ target."""
        return {
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {
                "name": f"{source_cluster_id}-failover-partition",
                "namespace": milvus_ns,
            },
            "spec": {
                "action": "partition",
                "mode": "all",
                "selector": {
                    "namespaces": [milvus_ns],
                    "labelSelectors": {"app.kubernetes.io/instance": source_cluster_id},
                },
                "direction": "both",
                "target": {
                    "mode": "all",
                    "selector": {
                        "namespaces": [milvus_ns],
                        "labelSelectors": {"app.kubernetes.io/instance": target_cluster_id},
                    },
                },
            },
        }

    def test_force_promote_with_network_partition(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        milvus_ns,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """Apply NetworkChaos partition between source/target, then force_promote.

        K8s-native scenario that the snippets tests can't reproduce. Simulates
        the realistic emergency that motivates force_promote: secondary cannot
        reach primary, operator force-promotes secondary.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_fp_partition", max_length=50)
        c_after = f"{c_name}_after"
        chaos_name = f"{source_cluster_id}-failover-partition"

        self.log_test_start(
            "test_force_promote_with_network_partition",
            "FORCE_PROMOTE_PARTITION",
            c_name,
        )
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client
        self.resources_to_cleanup.append(("collection", c_name))
        self.resources_to_cleanup.append(("collection", c_after))

        chaos_path = None
        try:
            self.cleanup_collection(upstream_client, c_name)
            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)
            assert self.wait_for_sync(
                lambda: downstream_client.has_collection(c_name),
                sync_timeout,
                f"create collection {c_name}",
            )

            data = self.generate_test_data_with_id(DEFAULT_INSERT_COUNT, start_id=0)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_synced():
                try:
                    res = downstream_client.query(collection_name=c_name, filter="", output_fields=["count(*)"])
                    return (res[0]["count(*)"] if res else 0) >= DEFAULT_INSERT_COUNT
                except Exception:
                    return False

            assert self.wait_for_sync(check_synced, sync_timeout, f"sync {c_name}")

            # Apply NetworkChaos
            manifest = self._build_partition_manifest(source_cluster_id, target_cluster_id, milvus_ns)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                yaml.safe_dump(manifest, f)
                chaos_path = f.name
            logger.info(f"[CHAOS] applying NetworkChaos partition: {chaos_path}")
            apply_result = subprocess.run(
                ["kubectl", "apply", "-f", chaos_path],
                capture_output=True,
                text=True,
                check=False,
            )
            assert apply_result.returncode == 0, f"kubectl apply failed: {apply_result.stderr}"

            # Let the partition propagate
            time.sleep(10)

            # Force promote downstream — it can't reach upstream
            self.promote_call_until_writable(
                downstream_client,
                target_cluster_id,
                downstream_uri,
                downstream_token,
                pchannel_num,
            )

            # Verify writable
            schema_after = self.create_manual_id_schema(downstream_client)
            downstream_client.create_collection(collection_name=c_after, schema=schema_after)
            idx_after = downstream_client.prepare_index_params()
            idx_after.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 16, "efConstruction": 200},
            )
            downstream_client.create_index(c_after, idx_after)
            downstream_client.load_collection(c_after)
            data_after = self.generate_test_data_with_id(100, start_id=0)
            downstream_client.insert(c_after, data_after)
            downstream_client.flush(c_after)
            res = downstream_client.query(collection_name=c_after, filter="", output_fields=["count(*)"])
            assert res and res[0]["count(*)"] >= 100, f"downstream not writable after partition+promote: {res}"
        finally:
            # Remove the partition first so switchover can fan-out across clusters
            logger.info(f"[CHAOS] cleaning up NetworkChaos {chaos_name}")
            subprocess.run(
                ["kubectl", "delete", "networkchaos", chaos_name, "-n", milvus_ns],
                capture_output=True,
                text=True,
                check=False,
            )
            if chaos_path and os.path.exists(chaos_path):
                os.unlink(chaos_path)
            # Allow partition to clear before switchover
            time.sleep(10)
            logger.info("[TEARDOWN] Restoring A→B topology...")
            try:
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as e:
                logger.warning(f"switchover restore failed: {e}")
            self.cleanup_resources()
            self.log_test_end(
                "test_force_promote_with_network_partition",
                True,
                time.time() - start_time,
            )

    def _do_one_endurance_iteration(
        self,
        iteration,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """One endurance iteration: DDL + insert + force_promote + DDL on new primary."""
        c_before = f"endurance_before_{iteration}"
        c_after = f"endurance_after_{iteration}"

        # Reset to A→B topology (in case prior iteration left state behind)
        try:
            switchover_helper(source_cluster_id, target_cluster_id)
        except Exception as e:
            logger.warning(f"[iter {iteration}] pre-iter switchover skipped: {e}")

        # DDL before failover on upstream
        self.cleanup_collection(upstream_client, c_before)
        schema = self.create_manual_id_schema(upstream_client)
        upstream_client.create_collection(collection_name=c_before, schema=schema)
        idx = upstream_client.prepare_index_params()
        idx.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 16, "efConstruction": 200},
        )
        upstream_client.create_index(c_before, idx)
        upstream_client.load_collection(c_before)
        assert self.wait_for_sync(
            lambda: downstream_client.has_collection(c_before),
            sync_timeout,
            f"[iter {iteration}] create {c_before}",
        )
        data = self.generate_test_data_with_id(100, start_id=0)
        upstream_client.insert(c_before, data)
        upstream_client.flush(c_before)

        def check_synced():
            try:
                res = downstream_client.query(collection_name=c_before, filter="", output_fields=["count(*)"])
                return (res[0]["count(*)"] if res else 0) >= 100
            except Exception:
                return False

        assert self.wait_for_sync(check_synced, sync_timeout, f"[iter {iteration}] sync {c_before}")

        # In-flight DDL
        upstream_client.release_collection(c_before)

        # Force promote
        self.promote_call_until_writable(
            downstream_client,
            target_cluster_id,
            downstream_uri,
            downstream_token,
            pchannel_num,
        )

        # DDL after failover on downstream
        schema_after = self.create_manual_id_schema(downstream_client)
        downstream_client.create_collection(collection_name=c_after, schema=schema_after)
        idx_after = downstream_client.prepare_index_params()
        idx_after.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 16, "efConstruction": 200},
        )
        downstream_client.create_index(c_after, idx_after)
        downstream_client.load_collection(c_after)
        data_after = self.generate_test_data_with_id(100, start_id=0)
        downstream_client.insert(c_after, data_after)
        downstream_client.flush(c_after)
        res = downstream_client.query(collection_name=c_after, filter="", output_fields=["count(*)"])
        assert res and res[0]["count(*)"] >= 100, f"[iter {iteration}] downstream count check failed: {res}"

        # Cleanup both
        try:
            downstream_client.drop_collection(c_after)
        except Exception as e:
            logger.warning(f"[iter {iteration}] drop {c_after} on downstream: {e}")
        try:
            downstream_client.drop_collection(c_before)
        except Exception as e:
            logger.warning(f"[iter {iteration}] drop {c_before} on downstream: {e}")

    @pytest.mark.skipif(
        not os.getenv("RUN_ENDURANCE"),
        reason="Endurance test only runs when RUN_ENDURANCE is set",
    )
    def test_endurance_force_promote(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
        upstream_uri,
        upstream_token,
        downstream_uri,
        downstream_token,
        pchannel_num,
    ):
        """Endurance loop: repeated force_promote + DDL for ENDURANCE_DURATION_MINUTES."""
        duration_minutes = int(os.getenv("ENDURANCE_DURATION_MINUTES", "30"))
        deadline = time.time() + duration_minutes * 60
        iteration = 0
        start_time = time.time()
        self.log_test_start("test_endurance_force_promote", "ENDURANCE_FORCE_PROMOTE", "")
        self._upstream_client = upstream_client
        self._downstream_client = downstream_client

        try:
            while time.time() < deadline:
                iteration += 1
                logger.info(f"=== Endurance iteration {iteration} ===")
                self._do_one_endurance_iteration(
                    iteration,
                    upstream_client,
                    downstream_client,
                    sync_timeout,
                    switchover_helper,
                    source_cluster_id,
                    target_cluster_id,
                    upstream_uri,
                    upstream_token,
                    downstream_uri,
                    downstream_token,
                    pchannel_num,
                )
            logger.info(f"PASSED endurance: {iteration} iterations in {duration_minutes}m")
        finally:
            logger.info("[TEARDOWN] Restoring A→B topology after endurance...")
            try:
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as e:
                logger.warning(f"switchover restore failed: {e}")
            self.log_test_end("test_endurance_force_promote", True, time.time() - start_time)
