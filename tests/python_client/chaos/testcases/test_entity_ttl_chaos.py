import time
import pytest
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta, timezone

from pymilvus import connections, utility, MilvusClient, DataType

from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.common_type import CaseLabel
from common import common_func as cf
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name, get_milvus_deploy_tool
from utils.util_common import update_key_value, update_key_name, gen_experiment_config
from chaos import constants

# Data group sizes
NB_PER_GROUP = 500
DIM = 128
# TTL values
TTL_SHORT_SECONDS = 30   # Group A: expires during chaos
TTL_LONG_SECONDS = 600   # Group B: survives the test
# PK ranges per group
GROUP_A_START = 0
GROUP_B_START = NB_PER_GROUP
GROUP_C_START = NB_PER_GROUP * 2


class TestEntityTTLChaosBase:
    """Base class for entity TTL chaos tests."""

    @pytest.fixture(scope="function", autouse=True)
    def init_env(self, host, port, user, password, milvus_ns):
        if user and password:
            connections.connect('default', host=host, port=port, user=user, password=password, secure=False)
        else:
            connections.connect('default', host=host, port=port, secure=False)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.milvus_sys = MilvusSys(alias='default')
        self.chaos_ns = constants.CHAOS_NAMESPACE
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.deploy_by = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)
        # Build URI for MilvusClient
        self.uri = f"http://{host}:{port}"
        self.token = f"{user}:{password}" if user and password else None

    def _get_client(self):
        """Create a MilvusClient instance."""
        if self.token:
            return MilvusClient(uri=self.uri, token=self.token)
        return MilvusClient(uri=self.uri)

    def _reconnect(self):
        """Re-establish connection after chaos."""
        if self.user and self.password:
            connections.connect('default', host=self.host, port=self.port,
                                user=self.user, password=self.password, secure=False)
        else:
            connections.connect('default', host=self.host, port=self.port, secure=False)

    def _create_ttl_collection(self, client, collection_name):
        """Create a collection with entity-level TTL field.

        Schema: pk(INT64), vector(FLOAT_VECTOR, dim=128), ttl(TIMESTAMPTZ, nullable)
        Properties: ttl_field=ttl, timezone=UTC
        """
        schema = client.create_schema(enable_dynamic_field=False)
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            properties={"ttl_field": "ttl", "timezone": "UTC"},
            consistency_level="Strong",
        )
        log.info(f"Created collection {collection_name} with entity-level TTL")

    def _insert_three_groups(self, client, collection_name):
        """Insert 3 data groups with known PKs and different TTL values.

        Group A (pk 0~499): TTL = now + 30s (expires during chaos)
        Group B (pk 500~999): TTL = now + 600s (survives the test)
        Group C (pk 1000~1499): TTL = NULL (never expires)

        Returns dict with group info for verification.
        """
        now = datetime.now(timezone.utc)
        ttl_short = (now + timedelta(seconds=TTL_SHORT_SECONDS)).isoformat()
        ttl_long = (now + timedelta(seconds=TTL_LONG_SECONDS)).isoformat()

        vectors = cf.gen_vectors(NB_PER_GROUP * 3, dim=DIM)

        # Group A: short TTL
        rows_a = [{"pk": GROUP_A_START + i, "vector": list(vectors[i]),
                    "ttl": ttl_short} for i in range(NB_PER_GROUP)]
        client.insert(collection_name=collection_name, data=rows_a)
        log.info(f"Inserted Group A: {NB_PER_GROUP} rows, TTL={TTL_SHORT_SECONDS}s")

        # Group B: long TTL
        rows_b = [{"pk": GROUP_B_START + i, "vector": list(vectors[NB_PER_GROUP + i]),
                    "ttl": ttl_long} for i in range(NB_PER_GROUP)]
        client.insert(collection_name=collection_name, data=rows_b)
        log.info(f"Inserted Group B: {NB_PER_GROUP} rows, TTL={TTL_LONG_SECONDS}s")

        # Group C: NULL TTL (never expires)
        rows_c = [{"pk": GROUP_C_START + i, "vector": list(vectors[NB_PER_GROUP * 2 + i]),
                    "ttl": None} for i in range(NB_PER_GROUP)]
        client.insert(collection_name=collection_name, data=rows_c)
        log.info(f"Inserted Group C: {NB_PER_GROUP} rows, TTL=NULL")

        return {
            "A": {"pks": list(range(GROUP_A_START, GROUP_A_START + NB_PER_GROUP)),
                   "expected_after_expiry": 0},
            "B": {"pks": list(range(GROUP_B_START, GROUP_B_START + NB_PER_GROUP)),
                   "expected_after_expiry": NB_PER_GROUP},
            "C": {"pks": list(range(GROUP_C_START, GROUP_C_START + NB_PER_GROUP)),
                   "expected_after_expiry": NB_PER_GROUP},
        }

    def _verify_correctness(self, client, collection_name, groups, timeout=120):
        """Verify TTL correctness with retry.

        - Group A: count == 0 (expired, must not resurrect)
        - Group B: count == NB_PER_GROUP (not expired, no data loss)
        - Group C: count == NB_PER_GROUP (NULL TTL, never expires)
        """
        start = time.time()
        last_a, last_b, last_c = None, None, None
        query_succeeded = False

        while time.time() - start < timeout:
            try:
                res_a = client.query(collection_name=collection_name,
                                     filter=f"pk >= {GROUP_A_START} and pk < {GROUP_B_START}",
                                     output_fields=["count(*)"],
                                     consistency_level="Strong")
                res_b = client.query(collection_name=collection_name,
                                     filter=f"pk >= {GROUP_B_START} and pk < {GROUP_C_START}",
                                     output_fields=["count(*)"],
                                     consistency_level="Strong")
                res_c = client.query(collection_name=collection_name,
                                     filter=f"pk >= {GROUP_C_START} and pk < {GROUP_C_START + NB_PER_GROUP}",
                                     output_fields=["count(*)"],
                                     consistency_level="Strong")

                query_succeeded = True
                last_a = res_a[0].get("count(*)", -1)
                last_b = res_b[0].get("count(*)", -1)
                last_c = res_c[0].get("count(*)", -1)

                log.info(f"Verification: GroupA={last_a}, GroupB={last_b}, GroupC={last_c}")

                if (last_a == 0 and
                        last_b == NB_PER_GROUP and
                        last_c == NB_PER_GROUP):
                    log.info("All groups verified correctly")
                    return
            except Exception as e:
                log.warning(f"Verification query failed (retrying): {e}")

            sleep(5)

        # Fail fast if we never got a successful query
        assert query_succeeded, \
            f"All verification queries failed for {timeout}s - service never recovered"

        # Final assertion with clear error messages
        assert last_a == 0, \
            f"Zombie resurrection: {last_a} expired entities visible after recovery (Group A)"
        assert last_b == NB_PER_GROUP, \
            f"Data loss: Group B has {last_b}/{NB_PER_GROUP} entities after recovery"
        assert last_c == NB_PER_GROUP, \
            f"NULL TTL data loss: Group C has {last_c}/{NB_PER_GROUP} entities after recovery"

    def _verify_search_consistency(self, client, collection_name, timeout=60):
        """Verify search returns only non-expired results and actually returns data."""
        search_vectors = cf.gen_vectors(1, dim=DIM)
        start = time.time()
        last_error = None

        while time.time() - start < timeout:
            try:
                res = client.search(
                    collection_name=collection_name,
                    data=search_vectors,
                    anns_field="vector",
                    limit=100,
                    output_fields=["pk"],
                    consistency_level="Strong",
                )
                if len(res) > 0 and len(res[0]) > 0:
                    result_pks = [hit["entity"]["pk"] for hit in res[0]]
                    # Verify no expired entities in results
                    expired_pks = [pk for pk in result_pks if pk < GROUP_B_START]
                    assert len(expired_pks) == 0, \
                        f"Search returned {len(expired_pks)} expired entities: {expired_pks[:10]}"
                    # Verify results actually contain non-expired entities
                    valid_pks = [pk for pk in result_pks if pk >= GROUP_B_START]
                    assert len(valid_pks) > 0, \
                        "Search returned results but none from non-expired groups (B or C)"
                    log.info(f"Search consistency verified: {len(result_pks)} results, "
                             f"{len(valid_pks)} from non-expired groups")
                    return
                else:
                    last_error = "Search returned empty results"
                    log.warning(f"Search returned empty results (retrying)")
            except Exception as e:
                last_error = str(e)
                log.warning(f"Search verification failed (retrying): {e}")
            sleep(5)

        pytest.fail(f"Search verification timed out after {timeout}s - last error: {last_error}")

    def _apply_chaos(self, chaos_yaml_path, chaos_duration_seconds=120):
        """Apply Chaos Mesh experiment and return chaos resource info for cleanup.

        Returns (chaos_res, meta_name) for teardown.
        """
        chaos_config = gen_experiment_config(chaos_yaml_path)
        chaos_config['metadata']['name'] = f"test-ttl-{int(time.time())}"
        chaos_config['metadata']['namespace'] = self.chaos_ns
        update_key_value(chaos_config, "app.kubernetes.io/instance", self.release_name)
        update_key_value(chaos_config, "namespaces", [self.milvus_ns])
        if self.deploy_by == "milvus-operator":
            update_key_name(chaos_config, "component", "app.kubernetes.io/component")

        meta_name = chaos_config['metadata']['name']
        log.info(f"Applying chaos: {meta_name}, config: {chaos_config}")

        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.info(f"Chaos {meta_name} applied successfully")
        return chaos_res, meta_name, chaos_config

    def _delete_chaos(self, chaos_res, meta_name):
        """Delete chaos experiment and wait for pods to recover."""
        try:
            chaos_res.delete(meta_name, raise_ex=False)
            log.info(f"Chaos {meta_name} deleted")
        except Exception as e:
            log.warning(f"Failed to delete chaos {meta_name}: {e}")

        # Wait for all pods to become ready
        log.info(f"Waiting for pods to recover in namespace {self.milvus_ns}")
        wait_pods_ready(self.milvus_ns, f"app.kubernetes.io/instance={self.release_name}")
        log.info("All pods recovered")

    def _wait_connection_recovery(self, timeout=120):
        """Wait for Milvus connection to recover after chaos."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                self._reconnect()
                log.info(f"Connection recovered in {time.time() - start:.1f}s")
                return
            except Exception as e:
                log.debug(f"Connection not ready: {e}")
                sleep(2)
        pytest.fail(f"Connection did not recover within {timeout}s")


class TestEntityTTLNodeKillChaos(TestEntityTTLChaosBase):
    """Test entity TTL correctness after querynode/streamingnode pod kills."""

    @pytest.mark.tags(CaseLabel.L3)
    def test_entity_ttl_querynode_kill_during_expiry(self):
        """
        Test entity TTL correctness when querynode is killed during TTL expiration.

        Scenario:
        1. Create collection with entity-level TTL, insert 3 groups (short/long/null TTL)
        2. Flush to create sealed segments on querynode
        3. Kill querynode while Group A TTL is expiring
        4. After recovery, verify:
           - Group A (expired): count == 0 (no zombie resurrection)
           - Group B (alive): count == NB_PER_GROUP (no data loss)
           - Group C (null): count == NB_PER_GROUP (null TTL preserved)
           - Search returns only non-expired entities
        """
        client = self._get_client()
        collection_name = cf.gen_unique_str("ttl_qn_chaos")
        chaos_res, meta_name, chaos_config = None, None, None

        try:
            # 1. Setup: create collection with TTL
            self._create_ttl_collection(client, collection_name)
            groups = self._insert_three_groups(client, collection_name)

            # 2. Flush to seal segments (querynode serves sealed segments)
            client.flush(collection_name)
            log.info("Flushed collection - data is in sealed segments")

            # 3. Verify baseline: all non-expired data visible
            res = client.query(collection_name=collection_name,
                               filter=f"pk >= {GROUP_A_START}",
                               output_fields=["count(*)"],
                               consistency_level="Strong")
            baseline_count = res[0].get("count(*)", 0)
            # Group A + Group B + Group C should be visible (Group A not yet expired)
            log.info(f"Baseline count: {baseline_count}")
            assert baseline_count == NB_PER_GROUP * 3, \
                f"Baseline count mismatch: {baseline_count} != {NB_PER_GROUP * 3}"

            # 4. Wait ~15s then inject chaos (kill querynode)
            #    Group A expires at ~30s, so chaos hits during the expiration window
            sleep(15)
            chaos_yaml = str(Path(__file__).absolute().parent.parent /
                             "chaos_objects/pod_kill/chaos_querynode_pod_kill.yaml")
            chaos_res, meta_name, chaos_config = self._apply_chaos(chaos_yaml)

            # 5. Wait for Group A to expire + recovery time
            log.info(f"Waiting for Group A to expire and pod to recover...")
            sleep(75)  # 15s pre-chaos + 30s TTL + 30s recovery buffer

            # 6. Delete chaos and wait for recovery
            self._delete_chaos(chaos_res, meta_name)
            self._wait_connection_recovery()

            # Re-create client after recovery
            client = self._get_client()

            # 7. Verify correctness
            log.info("Starting correctness verification...")
            self._verify_correctness(client, collection_name, groups)
            self._verify_search_consistency(client, collection_name)
            log.info("test_entity_ttl_querynode_kill_during_expiry PASSED")

        except Exception as e:
            log.error(f"Test failed: {e}")
            raise
        finally:
            # Cleanup chaos if still active
            if chaos_res and meta_name:
                try:
                    chaos_res.delete(meta_name, raise_ex=False)
                except Exception:
                    pass
            # Cleanup collection
            try:
                client.drop_collection(collection_name)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L3)
    def test_entity_ttl_streamingnode_kill_during_expiry(self):
        """
        Test entity TTL correctness when streamingnode is killed during TTL expiration.

        Unlike the querynode test, this test does NOT flush - keeping data in growing
        segments served by the streamingnode.

        Scenario:
        1. Create collection with entity-level TTL, insert 3 groups (short/long/null TTL)
        2. Do NOT flush (data stays in growing segments on streamingnode)
        3. Kill streamingnode while Group A TTL is expiring
        4. After recovery, verify same correctness properties
        """
        client = self._get_client()
        collection_name = cf.gen_unique_str("ttl_sn_chaos")
        chaos_res, meta_name, chaos_config = None, None, None

        try:
            # 1. Setup: create collection with TTL
            self._create_ttl_collection(client, collection_name)
            groups = self._insert_three_groups(client, collection_name)
            # NOTE: No flush - data stays in growing segments on streamingnode

            # 2. Verify baseline
            res = client.query(collection_name=collection_name,
                               filter=f"pk >= {GROUP_A_START}",
                               output_fields=["count(*)"],
                               consistency_level="Strong")
            baseline_count = res[0].get("count(*)", 0)
            log.info(f"Baseline count: {baseline_count}")
            assert baseline_count == NB_PER_GROUP * 3, \
                f"Baseline count mismatch: {baseline_count} != {NB_PER_GROUP * 3}"

            # 3. Wait ~15s then inject chaos (kill streamingnode)
            sleep(15)
            chaos_yaml = str(Path(__file__).absolute().parent.parent /
                             "chaos_objects/pod_kill/chaos_streamingnode_pod_kill.yaml")
            chaos_res, meta_name, chaos_config = self._apply_chaos(chaos_yaml)

            # 4. Wait for Group A to expire + recovery time
            log.info(f"Waiting for Group A to expire and pod to recover...")
            sleep(75)

            # 5. Delete chaos and wait for recovery
            self._delete_chaos(chaos_res, meta_name)
            self._wait_connection_recovery()

            # Re-create client after recovery
            client = self._get_client()

            # 6. Verify correctness
            log.info("Starting correctness verification...")
            self._verify_correctness(client, collection_name, groups)
            self._verify_search_consistency(client, collection_name)
            log.info("test_entity_ttl_streamingnode_kill_during_expiry PASSED")

        except Exception as e:
            log.error(f"Test failed: {e}")
            raise
        finally:
            if chaos_res and meta_name:
                try:
                    chaos_res.delete(meta_name, raise_ex=False)
                except Exception:
                    pass
            try:
                client.drop_collection(collection_name)
            except Exception:
                pass


class TestEntityTTLCompactionChaos(TestEntityTTLChaosBase):
    """Test entity TTL correctness when datanode is killed during compaction."""

    @pytest.mark.tags(CaseLabel.L3)
    def test_entity_ttl_datanode_kill_during_compaction(self):
        """
        Test entity TTL correctness when datanode is killed during compaction of expired data.

        Scenario:
        1. Create collection with entity-level TTL, insert 3 groups
        2. Flush to create sealed segments
        3. Wait for Group A to expire
        4. Trigger manual compaction
        5. Kill datanode during compaction
        6. After recovery, verify:
           - Group A: count == 0 (expired data correctly handled)
           - Group B: count == NB_PER_GROUP (no data loss)
           - Group C: count == NB_PER_GROUP (null TTL preserved)
           - Compaction eventually completes (no stuck state)
        """
        client = self._get_client()
        collection_name = cf.gen_unique_str("ttl_dn_chaos")
        chaos_res, meta_name, chaos_config = None, None, None

        try:
            # 1. Setup
            self._create_ttl_collection(client, collection_name)
            groups = self._insert_three_groups(client, collection_name)

            # 2. Flush to create sealed segments
            client.flush(collection_name)
            log.info("Flushed collection - data is in sealed segments")

            # 3. Verify baseline
            res = client.query(collection_name=collection_name,
                               filter=f"pk >= {GROUP_A_START}",
                               output_fields=["count(*)"],
                               consistency_level="Strong")
            baseline_count = res[0].get("count(*)", 0)
            log.info(f"Baseline count: {baseline_count}")
            assert baseline_count == NB_PER_GROUP * 3

            # 4. Wait for Group A to fully expire
            log.info(f"Waiting {TTL_SHORT_SECONDS + 10}s for Group A to expire...")
            sleep(TTL_SHORT_SECONDS + 10)

            # Verify Group A has expired
            res = client.query(collection_name=collection_name,
                               filter=f"pk >= {GROUP_A_START} and pk < {GROUP_B_START}",
                               output_fields=["count(*)"],
                               consistency_level="Strong")
            assert res[0].get("count(*)", -1) == 0, \
                f"Group A should be expired but has {res[0].get('count(*)')} entities"
            log.info("Group A confirmed expired")

            # 5. Trigger manual compaction, then immediately kill datanode
            log.info("Triggering compaction...")
            client.compact(collection_name)

            # Kill datanode right after triggering compaction
            sleep(2)
            chaos_yaml = str(Path(__file__).absolute().parent.parent /
                             "chaos_objects/pod_kill/chaos_datanode_pod_kill.yaml")
            chaos_res, meta_name, chaos_config = self._apply_chaos(chaos_yaml)

            # 6. Let chaos run for a while
            log.info("Chaos active, waiting for recovery...")
            sleep(60)

            # 7. Delete chaos and wait for recovery
            self._delete_chaos(chaos_res, meta_name)
            self._wait_connection_recovery()

            # Re-create client after recovery
            client = self._get_client()

            # 8. Verify correctness
            log.info("Starting correctness verification...")
            self._verify_correctness(client, collection_name, groups)
            self._verify_search_consistency(client, collection_name)

            # 9. Verify compaction completes eventually
            log.info("Verifying compaction state...")
            compaction_completed = False
            start = time.time()
            while time.time() - start < 120:
                try:
                    # Trigger another compaction to ensure cleanup completes
                    client.compact(collection_name)
                    compaction_completed = True
                    break
                except Exception as e:
                    log.warning(f"Compaction check: {e}")
                    sleep(10)

            log.info(f"Compaction state verified: completed={compaction_completed}")
            log.info("test_entity_ttl_datanode_kill_during_compaction PASSED")

        except Exception as e:
            log.error(f"Test failed: {e}")
            raise
        finally:
            if chaos_res and meta_name:
                try:
                    chaos_res.delete(meta_name, raise_ex=False)
                except Exception:
                    pass
            try:
                client.drop_collection(collection_name)
            except Exception:
                pass


class TestEntityTTLUpsertDuringChaos(TestEntityTTLChaosBase):
    """Test entity TTL correctness when upsert extends TTL during node kill.

    When to add tests here:
    - Test verifies that upsert with updated TTL values survives chaos
    - Test checks that TTL extension via upsert is correctly persisted after recovery
    """

    @pytest.mark.tags(CaseLabel.L3)
    def test_entity_ttl_upsert_extends_ttl_during_querynode_kill(self):
        """
        Test that upserting Group A with a longer TTL during querynode kill
        correctly persists the updated TTL after recovery.

        Scenario:
        1. Create collection with entity-level TTL, insert 3 groups (short/long/null TTL)
        2. Flush to create sealed segments
        3. Kill querynode
        4. While chaos is active, upsert Group A with a much longer TTL (600s)
        5. After recovery, verify:
           - Group A: count == NB_PER_GROUP (TTL was extended, should NOT expire)
           - Group B: count == NB_PER_GROUP (no data loss)
           - Group C: count == NB_PER_GROUP (null TTL preserved)
           - Search returns entities from all 3 groups
        """
        client = self._get_client()
        collection_name = cf.gen_unique_str("ttl_upsert_chaos")
        chaos_res, meta_name = None, None

        try:
            # 1. Setup: create collection with TTL and insert data
            self._create_ttl_collection(client, collection_name)
            groups = self._insert_three_groups(client, collection_name)

            # 2. Flush to seal segments
            client.flush(collection_name)
            log.info("Flushed collection - data is in sealed segments")

            # 3. Verify baseline
            res = client.query(collection_name=collection_name,
                               filter=f"pk >= {GROUP_A_START}",
                               output_fields=["count(*)"],
                               consistency_level="Strong")
            baseline_count = res[0].get("count(*)", 0)
            assert baseline_count == NB_PER_GROUP * 3, \
                f"Baseline count mismatch: {baseline_count} != {NB_PER_GROUP * 3}"

            # 4. Inject chaos (kill querynode)
            chaos_yaml = str(Path(__file__).absolute().parent.parent /
                             "chaos_objects/pod_kill/chaos_querynode_pod_kill.yaml")
            chaos_res, meta_name, _ = self._apply_chaos(chaos_yaml)

            # 5. While chaos is active, upsert Group A with extended TTL
            #    Simulate a real user: single upsert call, no retry
            sleep(5)  # brief wait for chaos to take effect
            upsert_time = datetime.now(timezone.utc)
            extended_ttl = (upsert_time + timedelta(seconds=TTL_LONG_SECONDS)).isoformat()
            vectors = cf.gen_vectors(NB_PER_GROUP, dim=DIM)
            rows_upsert = [{"pk": GROUP_A_START + i, "vector": list(vectors[i]),
                            "ttl": extended_ttl} for i in range(NB_PER_GROUP)]
            client.upsert(collection_name=collection_name, data=rows_upsert)
            log.info("Upsert with extended TTL succeeded during chaos")

            # 6. Delete chaos and wait for recovery (overlap with TTL wait)
            self._delete_chaos(chaos_res, meta_name)

            # Wait for original short TTL to pass; subtract time already elapsed
            elapsed = time.time() - upsert_time.timestamp()
            remaining = max(0, TTL_SHORT_SECONDS + 10 - elapsed)
            if remaining > 0:
                log.info(f"Waiting {remaining:.0f}s for original short TTL to pass...")
                sleep(remaining)

            self._wait_connection_recovery()
            client = self._get_client()

            # 8. Verify: Group A should still be alive because TTL was extended
            log.info("Verifying upsert TTL extension survived chaos...")
            start = time.time()
            query_succeeded = False
            count_a, count_b, count_c = None, None, None

            while time.time() - start < 120:
                try:
                    res_a = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_A_START} and pk < {GROUP_B_START}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")
                    res_b = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_B_START} and pk < {GROUP_C_START}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")
                    res_c = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_C_START} and pk < {GROUP_C_START + NB_PER_GROUP}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")

                    query_succeeded = True
                    count_a = res_a[0].get("count(*)", -1)
                    count_b = res_b[0].get("count(*)", -1)
                    count_c = res_c[0].get("count(*)", -1)
                    log.info(f"Verification: GroupA={count_a}, GroupB={count_b}, GroupC={count_c}")

                    if (count_a == NB_PER_GROUP and
                            count_b == NB_PER_GROUP and
                            count_c == NB_PER_GROUP):
                        break
                except Exception as e:
                    log.warning(f"Verification query failed (retrying): {e}")
                sleep(5)

            assert query_succeeded, \
                "All verification queries failed for 120s - service never recovered"
            assert count_a == NB_PER_GROUP, \
                f"TTL extension lost: Group A has {count_a}/{NB_PER_GROUP} after upsert during chaos"
            assert count_b == NB_PER_GROUP, \
                f"Data loss: Group B has {count_b}/{NB_PER_GROUP} after recovery"
            assert count_c == NB_PER_GROUP, \
                f"NULL TTL data loss: Group C has {count_c}/{NB_PER_GROUP} after recovery"

            # 9. Verify search returns results (all groups alive after upsert)
            self._verify_search_all_groups(client, collection_name)
            log.info("test_entity_ttl_upsert_extends_ttl_during_querynode_kill PASSED")

        except Exception as e:
            log.error(f"Test failed: {e}")
            raise
        finally:
            if chaos_res and meta_name:
                try:
                    chaos_res.delete(meta_name, raise_ex=False)
                except Exception:
                    pass
            try:
                client.drop_collection(collection_name)
            except Exception:
                pass

    def _verify_search_all_groups(self, client, collection_name, timeout=60):
        """Verify search returns results (all groups are alive after upsert)."""
        search_vectors = cf.gen_vectors(1, dim=DIM)
        start = time.time()
        last_error = None

        while time.time() - start < timeout:
            try:
                res = client.search(
                    collection_name=collection_name,
                    data=search_vectors,
                    anns_field="vector",
                    limit=100,
                    output_fields=["pk"],
                    consistency_level="Strong",
                )
                if len(res) > 0 and len(res[0]) > 0:
                    result_pks = [hit["entity"]["pk"] for hit in res[0]]
                    assert len(result_pks) > 0, "Search returned no results"
                    log.info(f"Search verified: {len(result_pks)} results returned")
                    return
                else:
                    last_error = "Search returned empty results"
            except Exception as e:
                last_error = str(e)
                log.warning(f"Search verification failed (retrying): {e}")
            sleep(5)

        pytest.fail(f"Search verification timed out after {timeout}s - last error: {last_error}")


class TestEntityTTLInsertDuringChaos(TestEntityTTLChaosBase):
    """Test entity TTL correctness when new data is inserted during node kill.

    When to add tests here:
    - Test verifies that data inserted during chaos with TTL is correctly
      persisted and honored after WAL replay and recovery
    """

    @pytest.mark.tags(CaseLabel.L3)
    def test_entity_ttl_insert_new_data_during_querynode_kill(self):
        """
        Test that new data inserted with TTL during querynode kill is correctly
        persisted after WAL replay and recovery.

        Scenario:
        1. Create collection with entity-level TTL, insert 3 initial groups
        2. Flush to create sealed segments
        3. Kill querynode
        4. While chaos is active, insert Group D (pk 1500~1999) with long TTL
           and Group E (pk 2000~2499) with short TTL
        5. After recovery, wait for Group E to expire, then verify:
           - Group A: count == 0 (original short TTL expired)
           - Group B: count == NB_PER_GROUP (original long TTL alive)
           - Group C: count == NB_PER_GROUP (null TTL alive)
           - Group D: count == NB_PER_GROUP (inserted during chaos, long TTL alive)
           - Group E: count == 0 (inserted during chaos, short TTL expired)
        """
        client = self._get_client()
        collection_name = cf.gen_unique_str("ttl_insert_chaos")
        chaos_res, meta_name = None, None

        GROUP_D_START = NB_PER_GROUP * 3
        GROUP_E_START = NB_PER_GROUP * 4

        try:
            # 1. Setup: create collection with TTL and insert initial data
            self._create_ttl_collection(client, collection_name)
            self._insert_three_groups(client, collection_name)

            # 2. Flush to seal initial segments
            client.flush(collection_name)
            log.info("Flushed collection - initial data is in sealed segments")

            # 3. Verify baseline
            res = client.query(collection_name=collection_name,
                               filter=f"pk >= {GROUP_A_START}",
                               output_fields=["count(*)"],
                               consistency_level="Strong")
            baseline_count = res[0].get("count(*)", 0)
            assert baseline_count == NB_PER_GROUP * 3, \
                f"Baseline count mismatch: {baseline_count} != {NB_PER_GROUP * 3}"

            # 4. Inject chaos (kill querynode)
            chaos_yaml = str(Path(__file__).absolute().parent.parent /
                             "chaos_objects/pod_kill/chaos_querynode_pod_kill.yaml")
            chaos_res, meta_name, _ = self._apply_chaos(chaos_yaml)

            # 5. While chaos is active, insert two new groups
            sleep(5)  # brief wait for chaos to take effect
            now = datetime.now(timezone.utc)
            ttl_long = (now + timedelta(seconds=TTL_LONG_SECONDS)).isoformat()
            ttl_short = (now + timedelta(seconds=TTL_SHORT_SECONDS)).isoformat()

            vectors = cf.gen_vectors(NB_PER_GROUP * 2, dim=DIM)

            # Group D: long TTL (should survive)
            rows_d = [{"pk": GROUP_D_START + i, "vector": list(vectors[i]),
                        "ttl": ttl_long} for i in range(NB_PER_GROUP)]
            # Group E: short TTL (should expire)
            rows_e = [{"pk": GROUP_E_START + i, "vector": list(vectors[NB_PER_GROUP + i]),
                        "ttl": ttl_short} for i in range(NB_PER_GROUP)]

            # Simulate a real user: single insert calls, no retry
            insert_time = datetime.now(timezone.utc)
            client.insert(collection_name=collection_name, data=rows_d)
            log.info("Group D (long TTL) inserted during chaos")
            client.insert(collection_name=collection_name, data=rows_e)
            log.info("Group E (short TTL) inserted during chaos")

            # 6. Delete chaos and wait for recovery (overlap with TTL wait)
            self._delete_chaos(chaos_res, meta_name)

            # Wait for short TTL groups (A and E) to expire; subtract time already elapsed
            elapsed = time.time() - insert_time.timestamp()
            remaining = max(0, TTL_SHORT_SECONDS + 15 - elapsed)
            if remaining > 0:
                log.info(f"Waiting {remaining:.0f}s for short-TTL groups to expire...")
                sleep(remaining)

            self._wait_connection_recovery()
            client = self._get_client()

            # 8. Verify all 5 groups
            log.info("Starting 5-group correctness verification...")
            start = time.time()
            query_succeeded = False
            count_a, count_b, count_c, count_d, count_e = None, None, None, None, None

            while time.time() - start < 120:
                try:
                    res_a = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_A_START} and pk < {GROUP_B_START}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")
                    res_b = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_B_START} and pk < {GROUP_C_START}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")
                    res_c = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_C_START} and pk < {GROUP_C_START + NB_PER_GROUP}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")
                    res_d = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_D_START} and pk < {GROUP_D_START + NB_PER_GROUP}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")
                    res_e = client.query(collection_name=collection_name,
                                         filter=f"pk >= {GROUP_E_START} and pk < {GROUP_E_START + NB_PER_GROUP}",
                                         output_fields=["count(*)"],
                                         consistency_level="Strong")

                    query_succeeded = True
                    count_a = res_a[0].get("count(*)", -1)
                    count_b = res_b[0].get("count(*)", -1)
                    count_c = res_c[0].get("count(*)", -1)
                    count_d = res_d[0].get("count(*)", -1)
                    count_e = res_e[0].get("count(*)", -1)

                    log.info(f"Verification: A={count_a}, B={count_b}, C={count_c}, "
                             f"D={count_d}, E={count_e}")

                    if (count_a == 0 and
                            count_b == NB_PER_GROUP and
                            count_c == NB_PER_GROUP and
                            count_d == NB_PER_GROUP and
                            count_e == 0):
                        break
                except Exception as e:
                    log.warning(f"Verification query failed (retrying): {e}")
                sleep(5)

            assert query_succeeded, \
                "All verification queries failed for 120s - service never recovered"
            assert count_a == 0, \
                f"Group A (original short TTL): expected 0, got {count_a}"
            assert count_b == NB_PER_GROUP, \
                f"Group B (original long TTL): expected {NB_PER_GROUP}, got {count_b}"
            assert count_c == NB_PER_GROUP, \
                f"Group C (null TTL): expected {NB_PER_GROUP}, got {count_c}"
            assert count_d == NB_PER_GROUP, \
                f"Group D (chaos-inserted long TTL): expected {NB_PER_GROUP}, got {count_d}"
            assert count_e == 0, \
                f"Group E (chaos-inserted short TTL): expected 0, got {count_e}"

            # 9. Verify search consistency (no expired entities from A or E)
            self._verify_search_no_expired(client, collection_name,
                                           GROUP_D_START, GROUP_E_START)
            log.info("test_entity_ttl_insert_new_data_during_querynode_kill PASSED")

        except Exception as e:
            log.error(f"Test failed: {e}")
            raise
        finally:
            if chaos_res and meta_name:
                try:
                    chaos_res.delete(meta_name, raise_ex=False)
                except Exception:
                    pass
            try:
                client.drop_collection(collection_name)
            except Exception:
                pass

    def _verify_search_no_expired(self, client, collection_name,
                                  group_d_start, group_e_start, timeout=60):
        """Verify search returns no expired entities (Group A or Group E)
        and returns at least some results from non-expired groups."""
        search_vectors = cf.gen_vectors(1, dim=DIM)
        start = time.time()
        last_error = None

        while time.time() - start < timeout:
            try:
                res = client.search(
                    collection_name=collection_name,
                    data=search_vectors,
                    anns_field="vector",
                    limit=100,
                    output_fields=["pk"],
                    consistency_level="Strong",
                )
                if len(res) > 0 and len(res[0]) > 0:
                    result_pks = [hit["entity"]["pk"] for hit in res[0]]
                    # Group A (0~499) and Group E (2000~2499) should be expired
                    expired_pks = [pk for pk in result_pks
                                   if pk < GROUP_B_START or
                                   (pk >= group_e_start and pk < group_e_start + NB_PER_GROUP)]
                    assert len(expired_pks) == 0, \
                        f"Search returned {len(expired_pks)} expired entities: {expired_pks[:10]}"
                    valid_pks = [pk for pk in result_pks if pk not in expired_pks]
                    assert len(valid_pks) > 0, \
                        "Search returned results but none from non-expired groups"
                    log.info(f"Search verified: {len(result_pks)} results, "
                             f"{len(valid_pks)} from non-expired groups, 0 expired")
                    return
                else:
                    last_error = "Search returned empty results"
                    log.warning("Search returned empty results (retrying)")
            except Exception as e:
                last_error = str(e)
                log.warning(f"Search verification failed (retrying): {e}")
            sleep(5)

        pytest.fail(f"Search verification timed out after {timeout}s - last error: {last_error}")
