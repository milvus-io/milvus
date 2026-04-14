"""
CDC sync tests for topology switchover and failover scenarios.
"""

import subprocess
import threading
import time
import random

from .base import TestCDCSyncBase, logger


class TestCDCSyncSwitchover(TestCDCSyncBase):
    """Test CDC sync behaviour during and after topology switchover / failover."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        upstream_client = getattr(self, "_upstream_client", None)

        if upstream_client:
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "collection":
                    self.cleanup_collection(upstream_client, resource_name)

            time.sleep(1)  # Allow cleanup to sync to downstream

    # -------------------------------------------------------------------------
    # Tests
    # -------------------------------------------------------------------------

    def test_switchover_basic(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Basic switchover: insert 200 records, verify sync, switchover, insert 200 more
        on the new source, verify count == 400 on both sides, sample verify, switch back.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_basic", max_length=50)

        self.log_test_start("test_switchover_basic", "SWITCHOVER_BASIC", c_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)

            # Wait for collection to appear on downstream
            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {c_name}"
            )

            # Insert 200 records on upstream (original source)
            batch1 = self.generate_test_data_with_id(200, start_id=0)
            upstream_client.insert(c_name, batch1)
            upstream_client.flush(c_name)

            def check_200():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {cnt}/200")
                    return cnt >= 200
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_200, sync_timeout, f"initial 200-record sync {c_name}"
            )

            # Switchover: target becomes new source
            logger.info("[SWITCHOVER] Initiating basic switchover...")
            switchover_helper(target_cluster_id, source_cluster_id)

            # Insert 200 more records on the new source (previously downstream)
            batch2 = self.generate_test_data_with_id(200, start_id=200)
            downstream_client.insert(c_name, batch2)
            downstream_client.flush(c_name)

            def check_400_upstream():
                try:
                    res = upstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] upstream count after switchover: {cnt}/400")
                    return cnt >= 400
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            def check_400_downstream():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count after switchover: {cnt}/400")
                    return cnt >= 400
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_400_upstream, sync_timeout, f"400-record sync upstream {c_name}"
            )
            assert self.wait_for_sync(
                check_400_downstream, sync_timeout, f"400-record sync downstream {c_name}"
            )

            # Sample verify
            match_count, mismatch_count, details = self.verify_data_sampling(
                downstream_client,  # new source
                upstream_client,    # new target
                c_name,
                sample_ratio=0.2,
                output_fields=["id", "vector"],
            )
            assert mismatch_count == 0, (
                f"Data mismatch after basic switchover: {details[:5]}"
            )

        finally:
            # Restore original topology
            logger.info("[SWITCHOVER] Restoring original topology after test_switchover_basic...")
            switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end("test_switchover_basic", True, time.time() - start_time)

    def test_switchover_during_writes(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Switchover during concurrent writes: background thread inserts 10 batches of 100
        with 2 s between each; switchover fires at t=12 s; join thread, flush, verify
        both sides have the same count >= total_inserted.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_writes", max_length=50)

        self.log_test_start(
            "test_switchover_during_writes", "SWITCHOVER_DURING_WRITES", c_name
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)

            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {c_name}"
            )

            total_inserted = 0
            lock = threading.Lock()
            write_error = []

            def background_insert():
                nonlocal total_inserted
                for batch_idx in range(10):
                    try:
                        start_id = batch_idx * 100
                        data = self.generate_test_data_with_id(100, start_id=start_id)
                        upstream_client.insert(c_name, data)
                        with lock:
                            total_inserted += 100
                        logger.info(
                            f"[BACKGROUND] Inserted batch {batch_idx + 1}/10 "
                            f"(total: {total_inserted})"
                        )
                    except Exception as e:
                        logger.error(f"[BACKGROUND] Insert failed on batch {batch_idx}: {e}")
                        write_error.append(e)
                    time.sleep(2)

            insert_thread = threading.Thread(target=background_insert, daemon=True)
            insert_thread.start()

            # Switchover after 12 s (mid-way through background inserts)
            time.sleep(12)
            logger.info("[SWITCHOVER] Initiating switchover during writes...")
            switchover_helper(target_cluster_id, source_cluster_id)

            insert_thread.join(timeout=60)
            assert not insert_thread.is_alive(), "Background insert thread did not finish in time"
            assert not write_error, f"Background inserts raised errors: {write_error}"

            upstream_client.flush(c_name)

            expected = total_inserted
            logger.info(f"[INFO] Total inserted: {expected}")

            def check_both(client, label):
                def _check():
                    try:
                        res = client.query(
                            collection_name=c_name,
                            filter="",
                            output_fields=["count(*)"],
                        )
                        cnt = res[0]["count(*)"] if res else 0
                        logger.info(f"[SYNC_PROGRESS] {label} count: {cnt}/{expected}")
                        return cnt >= expected
                    except Exception as e:
                        logger.warning(f"Sync check failed ({label}): {e}")
                        return False
                return _check

            assert self.wait_for_sync(
                check_both(upstream_client, "upstream"),
                sync_timeout,
                f"upstream count>={expected} after switchover-during-writes",
            )
            assert self.wait_for_sync(
                check_both(downstream_client, "downstream"),
                sync_timeout,
                f"downstream count>={expected} after switchover-during-writes",
            )

            up_res = upstream_client.query(
                collection_name=c_name, filter="", output_fields=["count(*)"]
            )
            down_res = downstream_client.query(
                collection_name=c_name, filter="", output_fields=["count(*)"]
            )
            up_cnt = up_res[0]["count(*)"] if up_res else 0
            down_cnt = down_res[0]["count(*)"] if down_res else 0
            assert up_cnt == down_cnt, (
                f"Count mismatch after switchover-during-writes: "
                f"upstream={up_cnt}, downstream={down_cnt}"
            )

        finally:
            logger.info("[SWITCHOVER] Restoring original topology after test_switchover_during_writes...")
            switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end(
                "test_switchover_during_writes", True, time.time() - start_time
            )

    def test_switchover_with_all_data_types(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Switchover with comprehensive data types: insert 100 records, verify sync,
        switchover, verify scalar field sampling, switch back.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_dtypes", max_length=50)

        self.log_test_start(
            "test_switchover_with_all_data_types",
            "SWITCHOVER_ALL_DTYPES",
            c_name,
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_comprehensive_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)

            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {c_name}"
            )

            data = self.generate_comprehensive_test_data(100)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_100():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {cnt}/100")
                    return cnt >= 100
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_100, sync_timeout, f"100-record sync {c_name}"
            )

            # Switchover
            logger.info("[SWITCHOVER] Initiating switchover with all data types...")
            switchover_helper(target_cluster_id, source_cluster_id)

            # After switchover, verify scalar field sampling (new source is downstream)
            scalar_fields = [
                "bool_field", "int8_field", "int16_field", "int32_field",
                "int64_field", "float_field", "double_field", "varchar_field",
            ]
            match_count, mismatch_count, details = self.verify_data_sampling(
                downstream_client,  # new source
                upstream_client,    # new target
                c_name,
                sample_ratio=0.3,
                output_fields=scalar_fields,
            )
            logger.info(
                f"[RESULT] All-dtypes sampling — match={match_count}, "
                f"mismatch={mismatch_count}"
            )
            assert mismatch_count == 0, (
                f"Data mismatch after switchover with all types: {details[:5]}"
            )

        finally:
            logger.info("[SWITCHOVER] Restoring original topology after test_switchover_with_all_data_types...")
            switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end(
                "test_switchover_with_all_data_types",
                True,
                time.time() - start_time,
            )

    def test_switchover_with_loaded_collection(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Switchover with a loaded collection: verify search works on downstream before
        switchover; after switchover verify search works on both sides.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_loaded", max_length=50)

        self.log_test_start(
            "test_switchover_with_loaded_collection",
            "SWITCHOVER_LOADED",
            c_name,
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_default_schema(upstream_client)
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

            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {c_name}"
            )

            data = self.generate_test_data(200)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_200():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {cnt}/200")
                    return cnt >= 200
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_200, sync_timeout, f"200-record sync {c_name}"
            )

            # Verify search works on downstream before switchover
            q_vec = [random.random() for _ in range(128)]
            pre_down_results = downstream_client.search(
                collection_name=c_name,
                data=[q_vec],
                anns_field="vector",
                search_params={"metric_type": "L2"},
                limit=5,
                output_fields=["id"],
            )
            assert pre_down_results and len(pre_down_results[0]) > 0, (
                "Search on downstream returned no results before switchover"
            )

            # Switchover
            logger.info("[SWITCHOVER] Initiating switchover with loaded collection...")
            switchover_helper(target_cluster_id, source_cluster_id)

            # Verify search still works on both sides after switchover
            for client, label in [(upstream_client, "upstream"), (downstream_client, "downstream")]:
                results = client.search(
                    collection_name=c_name,
                    data=[q_vec],
                    anns_field="vector",
                    search_params={"metric_type": "L2"},
                    limit=5,
                    output_fields=["id"],
                )
                assert results and len(results[0]) > 0, (
                    f"Search returned no results on {label} after switchover"
                )
                logger.info(
                    f"[VERIFY] Search on {label} after switchover returned "
                    f"{len(results[0])} results — OK"
                )

        finally:
            logger.info("[SWITCHOVER] Restoring original topology after test_switchover_with_loaded_collection...")
            switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end(
                "test_switchover_with_loaded_collection",
                True,
                time.time() - start_time,
            )

    def test_switchover_with_index(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Switchover after index creation: wait for index to sync, switchover, verify
        list_indexes returns the same index on both sides.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_index", max_length=50)

        self.log_test_start(
            "test_switchover_with_index", "SWITCHOVER_INDEX", c_name
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_default_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)

            # Insert data first
            data = self.generate_test_data(200)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            # Create HNSW index
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="COSINE",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)

            # Wait for index to sync to downstream
            def check_index_sync():
                try:
                    indexes = downstream_client.list_indexes(c_name)
                    result = len(indexes) > 0
                    logger.info(
                        f"[SYNC_PROGRESS] downstream indexes: {indexes} (has_index={result})"
                    )
                    return result
                except Exception as e:
                    logger.warning(f"Index sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_index_sync, sync_timeout, f"index sync {c_name}"
            ), f"Index did not sync to downstream within {sync_timeout}s"

            # Switchover
            logger.info("[SWITCHOVER] Initiating switchover with index...")
            switchover_helper(target_cluster_id, source_cluster_id)

            # Verify list_indexes on both sides
            up_indexes = upstream_client.list_indexes(c_name)
            down_indexes = downstream_client.list_indexes(c_name)

            logger.info(
                f"[VERIFY] After switchover — upstream indexes={up_indexes}, "
                f"downstream indexes={down_indexes}"
            )
            assert len(up_indexes) > 0, "No indexes on upstream after switchover"
            assert len(down_indexes) > 0, "No indexes on downstream after switchover"
            assert set(up_indexes) == set(down_indexes), (
                f"Index lists differ after switchover: up={up_indexes}, down={down_indexes}"
            )

        finally:
            logger.info("[SWITCHOVER] Restoring original topology after test_switchover_with_index...")
            switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end(
                "test_switchover_with_index", True, time.time() - start_time
            )

    def test_rapid_switchover_stress(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """
        Rapid-switchover stress: loop 5 times — write 50 records to current source,
        switchover. After loop, wait 30 s and verify counts match. Restore original topology.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_sw_stress", max_length=50)

        self.log_test_start(
            "test_rapid_switchover_stress", "RAPID_SWITCHOVER_STRESS", c_name
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)

            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="COSINE",
                params={"M": 16, "efConstruction": 200},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)

            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {c_name}"
            )

            # Track topology state
            current_source_id = source_cluster_id
            current_target_id = target_cluster_id
            current_source_client = upstream_client
            total_inserted = 0

            for iteration in range(5):
                # Write 50 records to current source
                start_id = iteration * 50
                data = self.generate_test_data_with_id(50, start_id=start_id)
                current_source_client.insert(c_name, data)
                current_source_client.flush(c_name)
                total_inserted += 50

                logger.info(
                    f"[STRESS] Iteration {iteration + 1}/5: inserted 50 records "
                    f"(total={total_inserted}), switching over..."
                )

                # Switchover
                switchover_helper(current_target_id, current_source_id)

                # Swap topology
                current_source_id, current_target_id = current_target_id, current_source_id
                current_source_client = (
                    downstream_client
                    if current_source_id == target_cluster_id
                    else upstream_client
                )

            logger.info(
                f"[STRESS] All 5 switchovers done. Waiting 30s for final sync "
                f"(total_inserted={total_inserted})..."
            )
            time.sleep(30)

            # Verify counts match on both sides
            up_res = upstream_client.query(
                collection_name=c_name, filter="", output_fields=["count(*)"]
            )
            down_res = downstream_client.query(
                collection_name=c_name, filter="", output_fields=["count(*)"]
            )
            up_cnt = up_res[0]["count(*)"] if up_res else 0
            down_cnt = down_res[0]["count(*)"] if down_res else 0

            logger.info(
                f"[VERIFY] After stress — upstream={up_cnt}, downstream={down_cnt}, "
                f"expected>={total_inserted}"
            )
            assert up_cnt >= total_inserted, (
                f"Upstream count {up_cnt} < expected {total_inserted} after stress"
            )
            assert down_cnt >= total_inserted, (
                f"Downstream count {down_cnt} < expected {total_inserted} after stress"
            )
            assert up_cnt == down_cnt, (
                f"Count mismatch after stress: upstream={up_cnt}, downstream={down_cnt}"
            )

        finally:
            # Restore to original topology
            logger.info(
                "[SWITCHOVER] Restoring original topology after test_rapid_switchover_stress..."
            )
            switchover_helper(source_cluster_id, target_cluster_id)
            self.log_test_end(
                "test_rapid_switchover_stress", True, time.time() - start_time
            )

    def test_failover_source_down(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
        milvus_ns,
    ):
        """
        Failover when source goes down: insert 500 records, verify sync, kill source pods,
        verify target count >= 500, wait for source to recover and verify count >= 500.
        """
        start_time = time.time()
        c_name = self.gen_unique_name("test_failover_src", max_length=50)

        self.log_test_start("test_failover_source_down", "FAILOVER_SOURCE_DOWN", c_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            schema = self.create_manual_id_schema(upstream_client)
            upstream_client.create_collection(collection_name=c_name, schema=schema)

            def check_create():
                return downstream_client.has_collection(c_name)

            assert self.wait_for_sync(
                check_create, sync_timeout, f"create collection {c_name}"
            )

            # Insert 500 records
            data = self.generate_test_data_with_id(500, start_id=0)
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            def check_500_downstream():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {cnt}/500")
                    return cnt >= 500
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_500_downstream, sync_timeout, f"500-record sync {c_name}"
            ), f"Downstream did not receive 500 records within {sync_timeout}s"

            # Kill source pods forcefully
            logger.info(
                f"[FAILOVER] Killing source pods "
                f"(instance={source_cluster_id}, ns={milvus_ns})..."
            )
            kill_cmd = [
                "kubectl",
                "delete",
                "pods",
                "-l",
                f"app.kubernetes.io/instance={source_cluster_id}",
                "-n",
                milvus_ns,
                "--grace-period=0",
                "--force",
            ]
            result = subprocess.run(kill_cmd, capture_output=True, text=True)
            logger.info(
                f"[FAILOVER] kubectl output: stdout={result.stdout!r}, "
                f"stderr={result.stderr!r}, rc={result.returncode}"
            )

            # Wait 60 s and verify target still has data
            logger.info("[FAILOVER] Waiting 60s after pod kill...")
            time.sleep(60)

            def check_target_intact():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[FAILOVER] Target count after kill: {cnt}")
                    return cnt >= 500
                except Exception as e:
                    logger.warning(f"Target check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_target_intact, 30, f"target intact after source kill {c_name}"
            ), "Target lost data after source pod kill"

            # Wait 120 s more for source to recover, then verify
            logger.info("[FAILOVER] Waiting 120s for source recovery...")
            time.sleep(120)

            def check_source_recovered():
                try:
                    res = upstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[FAILOVER] Source count after recovery: {cnt}")
                    return cnt >= 500
                except Exception as e:
                    logger.warning(f"Source recovery check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_source_recovered, 60, f"source recovery {c_name}"
            ), "Source did not recover with expected data count within timeout"

        finally:
            self.log_test_end(
                "test_failover_source_down", True, time.time() - start_time
            )
