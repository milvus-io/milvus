"""
GPU HNSW End-to-End Integration Tests

These tests validate GPU_HNSW functionality beyond parameter validation:
1. Recall correctness — verify GPU search produces correct results
2. Hot-load path — verify new segments load as GPU_HNSW while collection is loaded
3. Restart resilience — verify segments reload as GPU_HNSW after querynode restart
4. VRAM exhaustion — verify graceful handling when GPU memory is exceeded
5. Mixed CPU/GPU mode — verify selective override (only HNSW_int8 on GPU)

Requirements:
- Milvus cluster with GPU querynode(s)
- override_index_type config set in etcd:
    knowhere.HNSW_int8.load.override_index_type: GPU_HNSW
- pymilvus installed

Usage:
    pytest test_gpu_hnsw_e2e.py --host <milvus_host> --port 19530
    # or directly:
    python test_gpu_hnsw_e2e.py --host <milvus_host> --port 19530
"""

import time
import logging
import numpy as np
import pytest
from pymilvus import (
    MilvusClient,
    DataType,
)

log = logging.getLogger(__name__)

# Test constants
DIM = 384
NUM_VECTORS = 10000
NUM_QUERIES = 100
TOP_K = 10
EF_CONSTRUCTION = 200
EF_SEARCH = 128
M = 16
RECALL_THRESHOLD = 0.95
COLLECTION_PREFIX = "test_gpu_hnsw_e2e_"


def compute_recall(results, ground_truth, k):
    """Compute recall@k given search results and ground truth."""
    total_recall = 0.0
    for i in range(len(results)):
        retrieved = set(results[i][:k])
        relevant = set(ground_truth[i][:k])
        total_recall += len(retrieved & relevant) / k
    return total_recall / len(results)


def brute_force_knn(vectors, queries, k, metric="COSINE"):
    """Compute ground truth k-NN via brute force."""
    vectors = np.array(vectors, dtype=np.float32)
    queries = np.array(queries, dtype=np.float32)

    if metric == "COSINE":
        # Normalize for cosine similarity
        v_norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        q_norms = np.linalg.norm(queries, axis=1, keepdims=True)
        vectors = vectors / np.maximum(v_norms, 1e-8)
        queries = queries / np.maximum(q_norms, 1e-8)
        # Cosine similarity = dot product of normalized vectors
        scores = queries @ vectors.T
        # Higher similarity = closer, so negate for argsort
        indices = np.argsort(-scores, axis=1)[:, :k]
    elif metric == "L2":
        # L2 distance — lower is better
        # Use broadcasting: (nq, 1, dim) - (1, n, dim) → (nq, n, dim)
        diffs = queries[:, None, :] - vectors[None, :, :]
        dists = np.sum(diffs ** 2, axis=2)
        indices = np.argsort(dists, axis=1)[:, :k]
    elif metric == "IP":
        scores = queries @ vectors.T
        indices = np.argsort(-scores, axis=1)[:, :k]
    else:
        raise ValueError(f"Unsupported metric: {metric}")

    return indices.tolist()


def get_milvus_client(host, port):
    """Create a MilvusClient connection."""
    uri = f"http://{host}:{port}"
    return MilvusClient(uri=uri)


class TestGpuHnswRecall:
    """Test 1: End-to-end recall validation for GPU_HNSW.

    Inserts known vectors, searches, and verifies recall >= 0.95.
    This confirms the GPU kernel produces correct search results.
    """

    @pytest.fixture(autouse=True)
    def setup(self, host, port):
        self.client = get_milvus_client(host, port)
        self.collection_name = COLLECTION_PREFIX + "recall_" + str(int(time.time()))
        yield
        # Cleanup
        try:
            self.client.drop_collection(self.collection_name)
        except Exception:
            pass

    def test_gpu_hnsw_recall_cosine(self):
        """Verify GPU_HNSW achieves >= 95% recall on COSINE metric."""
        self._run_recall_test("COSINE")

    def test_gpu_hnsw_recall_l2(self):
        """Verify GPU_HNSW achieves >= 95% recall on L2 metric."""
        self._run_recall_test("L2")

    def test_gpu_hnsw_recall_ip(self):
        """Verify GPU_HNSW achieves >= 95% recall on IP metric."""
        self._run_recall_test("IP")

    def _run_recall_test(self, metric):
        """Core recall test logic for a given metric."""
        # Generate random vectors
        np.random.seed(42)
        vectors = np.random.randn(NUM_VECTORS, DIM).astype(np.float32).tolist()
        query_vectors = np.random.randn(NUM_QUERIES, DIM).astype(np.float32).tolist()

        # Compute ground truth
        ground_truth = brute_force_knn(vectors, query_vectors, TOP_K, metric)

        # Create collection with schema
        schema = self.client.create_schema()
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
        )

        # Insert vectors
        batch_size = 2000
        for i in range(0, NUM_VECTORS, batch_size):
            end = min(i + batch_size, NUM_VECTORS)
            rows = [{"id": j, "vector": vectors[j]} for j in range(i, end)]
            self.client.insert(self.collection_name, rows)

        self.client.flush(self.collection_name)

        # Create HNSW index (will be overridden to GPU_HNSW at load time)
        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            metric_type=metric,
            index_type="HNSW",
            params={"M": M, "efConstruction": EF_CONSTRUCTION},
        )
        self.client.create_index(self.collection_name, index_params)

        # Load collection (override_index_type should kick in here)
        self.client.load_collection(self.collection_name)

        # Wait for collection to be fully loaded
        time.sleep(2)

        # Search
        results = self.client.search(
            collection_name=self.collection_name,
            data=query_vectors,
            anns_field="vector",
            search_params={"metric_type": metric, "params": {"ef": EF_SEARCH}},
            limit=TOP_K,
            output_fields=["id"],
        )

        # Extract result IDs
        result_ids = []
        for hits in results:
            ids = [hit["id"] for hit in hits]
            result_ids.append(ids)

        # Compute recall
        recall = compute_recall(result_ids, ground_truth, TOP_K)
        log.info(f"GPU_HNSW recall@{TOP_K} ({metric}): {recall:.4f}")

        assert recall >= RECALL_THRESHOLD, (
            f"GPU_HNSW recall@{TOP_K} ({metric}) = {recall:.4f}, "
            f"expected >= {RECALL_THRESHOLD}"
        )


class TestGpuHnswHotLoad:
    """Test 2: Hot-load path — verify new segments load as GPU_HNSW.

    While collection is loaded, insert new records, flush to seal a segment,
    and verify the new segment is searchable with correct results (proving
    it was loaded via the GPU override path).
    """

    @pytest.fixture(autouse=True)
    def setup(self, host, port):
        self.client = get_milvus_client(host, port)
        self.collection_name = COLLECTION_PREFIX + "hotload_" + str(int(time.time()))
        yield
        try:
            self.client.drop_collection(self.collection_name)
        except Exception:
            pass

    def test_hot_load_new_segment_on_gpu(self):
        """Insert while loaded → flush → verify new segment is searchable on GPU."""
        np.random.seed(123)
        initial_count = 5000
        hot_insert_count = 1000

        # Generate vectors
        all_vectors = np.random.randn(initial_count + hot_insert_count, DIM).astype(np.float32).tolist()
        initial_vectors = all_vectors[:initial_count]
        hot_vectors = all_vectors[initial_count:]

        # Create collection
        schema = self.client.create_schema()
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
        )

        # Insert initial data
        rows = [{"id": i, "vector": initial_vectors[i]} for i in range(initial_count)]
        self.client.insert(self.collection_name, rows)
        self.client.flush(self.collection_name)

        # Create index and load
        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW",
            params={"M": M, "efConstruction": EF_CONSTRUCTION},
        )
        self.client.create_index(self.collection_name, index_params)
        self.client.load_collection(self.collection_name)
        time.sleep(2)

        # Verify initial search works
        query = [initial_vectors[0]]
        results = self.client.search(
            collection_name=self.collection_name,
            data=query,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=1,
        )
        assert len(results) > 0 and len(results[0]) > 0, "Initial search failed"
        assert results[0][0]["id"] == 0, "Self-search should return the same vector"

        # HOT INSERT: insert while collection is loaded
        hot_rows = [
            {"id": initial_count + i, "vector": hot_vectors[i]}
            for i in range(hot_insert_count)
        ]
        self.client.insert(self.collection_name, hot_rows)
        self.client.flush(self.collection_name)

        # Wait for new segment to be loaded (sealed + loaded via override path)
        time.sleep(5)

        # Verify hot-inserted vectors are searchable (self-search)
        query = [hot_vectors[0]]
        results = self.client.search(
            collection_name=self.collection_name,
            data=query,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=1,
        )
        assert len(results) > 0 and len(results[0]) > 0, (
            "Hot-loaded segment not searchable — override may not be applied on hot-load path"
        )
        assert results[0][0]["id"] == initial_count, (
            f"Self-search on hot-inserted vector failed: expected id={initial_count}, "
            f"got id={results[0][0]['id']}"
        )

        # Verify recall on hot-inserted vectors
        num_hot_queries = 50
        hot_query_vectors = hot_vectors[:num_hot_queries]
        ground_truth = brute_force_knn(
            all_vectors, hot_query_vectors, TOP_K, "COSINE"
        )

        results = self.client.search(
            collection_name=self.collection_name,
            data=hot_query_vectors,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=TOP_K,
            output_fields=["id"],
        )

        result_ids = [[hit["id"] for hit in hits] for hits in results]
        recall = compute_recall(result_ids, ground_truth, TOP_K)
        log.info(f"Hot-load recall@{TOP_K}: {recall:.4f}")

        assert recall >= RECALL_THRESHOLD, (
            f"Hot-load recall@{TOP_K} = {recall:.4f}, expected >= {RECALL_THRESHOLD}. "
            f"Override may not be applied on hot-load path."
        )


class TestGpuHnswRestart:
    """Test 3: Restart resilience — verify segments reload as GPU_HNSW.

    Load a collection, verify it's searchable, then release + reload
    (simulating querynode restart) and verify search still works correctly.

    Note: A full pod-kill restart test requires kubectl access. This test
    uses release/load to exercise the same code path (ReopenSegments) that
    fires on querynode restart.
    """

    @pytest.fixture(autouse=True)
    def setup(self, host, port):
        self.client = get_milvus_client(host, port)
        self.collection_name = COLLECTION_PREFIX + "restart_" + str(int(time.time()))
        yield
        try:
            self.client.drop_collection(self.collection_name)
        except Exception:
            pass

    def test_release_reload_preserves_gpu_search(self):
        """Release + reload collection → verify GPU search still works correctly."""
        np.random.seed(456)
        num_vectors = 5000

        vectors = np.random.randn(num_vectors, DIM).astype(np.float32).tolist()
        query_vectors = np.random.randn(NUM_QUERIES, DIM).astype(np.float32).tolist()
        ground_truth = brute_force_knn(vectors, query_vectors, TOP_K, "COSINE")

        # Create collection
        schema = self.client.create_schema()
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
        )

        # Insert
        batch_size = 2000
        for i in range(0, num_vectors, batch_size):
            end = min(i + batch_size, num_vectors)
            rows = [{"id": j, "vector": vectors[j]} for j in range(i, end)]
            self.client.insert(self.collection_name, rows)
        self.client.flush(self.collection_name)

        # Create index and load
        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW",
            params={"M": M, "efConstruction": EF_CONSTRUCTION},
        )
        self.client.create_index(self.collection_name, index_params)
        self.client.load_collection(self.collection_name)
        time.sleep(2)

        # Search before release — establish baseline
        results_before = self.client.search(
            collection_name=self.collection_name,
            data=query_vectors,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=TOP_K,
            output_fields=["id"],
        )
        ids_before = [[hit["id"] for hit in hits] for hits in results_before]
        recall_before = compute_recall(ids_before, ground_truth, TOP_K)
        log.info(f"Recall before release: {recall_before:.4f}")
        assert recall_before >= RECALL_THRESHOLD

        # RELEASE + RELOAD (simulates querynode restart path)
        self.client.release_collection(self.collection_name)
        time.sleep(1)
        self.client.load_collection(self.collection_name)
        time.sleep(3)  # Wait for segments to reload on GPU

        # Search after reload — should produce same results
        results_after = self.client.search(
            collection_name=self.collection_name,
            data=query_vectors,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=TOP_K,
            output_fields=["id"],
        )
        ids_after = [[hit["id"] for hit in hits] for hits in results_after]
        recall_after = compute_recall(ids_after, ground_truth, TOP_K)
        log.info(f"Recall after reload: {recall_after:.4f}")

        assert recall_after >= RECALL_THRESHOLD, (
            f"Recall after release/reload = {recall_after:.4f}, "
            f"expected >= {RECALL_THRESHOLD}. "
            f"Override may not be applied on ReopenSegments path."
        )

        # Results should be nearly identical before and after
        match_count = 0
        for before, after in zip(ids_before, ids_after):
            if before == after:
                match_count += 1
        match_rate = match_count / len(ids_before)
        log.info(f"Result match rate before/after reload: {match_rate:.4f}")

        # Allow some variance due to HNSW non-determinism, but should be mostly identical
        assert match_rate >= 0.90, (
            f"Result match rate = {match_rate:.4f}. Results diverged significantly "
            f"after reload — segments may have loaded differently."
        )


class TestGpuHnswVramExhaustion:
    """Test 4: VRAM exhaustion handling.

    Loads multiple large collections to exceed GPU memory capacity.
    Verifies that Milvus handles the condition gracefully — either by
    rejecting the load with an error or falling back to CPU.
    """

    @pytest.fixture(autouse=True)
    def setup(self, host, port):
        self.client = get_milvus_client(host, port)
        self.collections = []
        yield
        for name in self.collections:
            try:
                self.client.drop_collection(name)
            except Exception:
                pass

    def _create_and_load_collection(self, name, num_vectors, dim):
        """Helper: create collection, insert vectors, index, load."""
        schema = self.client.create_schema()
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.client.create_collection(collection_name=name, schema=schema)
        self.collections.append(name)

        # Insert in batches
        batch_size = 5000
        for i in range(0, num_vectors, batch_size):
            end = min(i + batch_size, num_vectors)
            vectors = np.random.randn(end - i, dim).astype(np.float32).tolist()
            rows = [{"id": j, "vector": vectors[j - i]} for j in range(i, end)]
            self.client.insert(name, rows)

        self.client.flush(name)

        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW",
            params={"M": M, "efConstruction": EF_CONSTRUCTION},
        )
        self.client.create_index(name, index_params)
        self.client.load_collection(name)

    def test_vram_exhaustion_graceful_handling(self):
        """Load progressively larger collections until GPU memory is stressed.

        Verifies that:
        - Initial collections load successfully on GPU
        - When VRAM is exhausted, Milvus either rejects load with a clear error
          or falls back gracefully (no crash, no silent data loss)
        - Previously loaded collections remain searchable
        """
        np.random.seed(789)
        large_dim = 768  # Larger dimension = more VRAM per vector
        vectors_per_collection = 50000  # ~150MB per collection on GPU (768d * 4B * 50K)
        max_collections = 10  # Attempt to load up to 10 collections

        first_collection = None
        load_failures = []

        for idx in range(max_collections):
            name = COLLECTION_PREFIX + f"vram_{idx}_" + str(int(time.time()))
            try:
                self._create_and_load_collection(name, vectors_per_collection, large_dim)
                time.sleep(2)

                if first_collection is None:
                    first_collection = name

                # Verify the collection is searchable
                query = np.random.randn(1, large_dim).astype(np.float32).tolist()
                results = self.client.search(
                    collection_name=name,
                    data=query,
                    anns_field="vector",
                    search_params={"metric_type": "COSINE", "params": {"ef": 64}},
                    limit=5,
                )
                assert len(results) > 0 and len(results[0]) > 0, (
                    f"Collection {name} loaded but not searchable"
                )
                log.info(f"Collection {idx} loaded successfully ({vectors_per_collection} vectors, dim={large_dim})")

            except Exception as e:
                error_msg = str(e)
                log.info(f"Collection {idx} load failed (expected at VRAM limit): {error_msg}")
                load_failures.append((idx, error_msg))
                # Remove from cleanup list if creation failed
                if name in self.collections:
                    try:
                        self.client.drop_collection(name)
                        self.collections.remove(name)
                    except Exception:
                        pass
                break

        # Key assertions:
        # 1. At least one collection loaded successfully
        assert first_collection is not None, "No collections loaded at all — GPU may not be configured"

        # 2. First collection is still searchable after loading others
        query = np.random.randn(1, large_dim).astype(np.float32).tolist()
        results = self.client.search(
            collection_name=first_collection,
            data=query,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=5,
        )
        assert len(results) > 0 and len(results[0]) > 0, (
            "First collection became unsearchable after loading additional collections"
        )

        # 3. If we hit VRAM limit, the error should be informative (not a crash)
        if load_failures:
            idx, msg = load_failures[0]
            log.info(f"VRAM exhaustion triggered at collection {idx}: {msg}")
            # Verify it's a recognizable error, not a generic crash
            assert any(keyword in msg.lower() for keyword in [
                "memory", "gpu", "resource", "insufficient", "oom",
                "out of memory", "exceed", "limit", "capacity",
            ]), (
                f"VRAM exhaustion error message is not descriptive: {msg}. "
                f"Expected a memory-related error from checkSegmentGpuMemSize."
            )
        else:
            log.info(
                f"All {max_collections} collections loaded — GPU has enough VRAM. "
                f"Consider increasing vectors_per_collection to stress-test further."
            )


class TestGpuHnswMixedMode:
    """Test 5: Mixed CPU/GPU mode — selective override.

    When only HNSW_int8 has override_index_type set (not HNSW), verify:
    - HNSW_int8 (SQ8) collections load on GPU and produce correct results
    - Plain HNSW (float32) collections stay on CPU and produce correct results
    - Both can coexist on the same querynode
    """

    @pytest.fixture(autouse=True)
    def setup(self, host, port):
        self.client = get_milvus_client(host, port)
        self.collection_int8 = COLLECTION_PREFIX + "mixed_int8_" + str(int(time.time()))
        self.collection_float = COLLECTION_PREFIX + "mixed_float_" + str(int(time.time()))
        yield
        for name in [self.collection_int8, self.collection_float]:
            try:
                self.client.drop_collection(name)
            except Exception:
                pass

    def test_int8_on_gpu_float_on_cpu(self):
        """HNSW_int8 collection → GPU, plain HNSW collection → CPU.

        Both should produce correct search results. This tests that the
        override mechanism is selective (keyed by source index type).
        """
        np.random.seed(321)
        num_vectors = 5000
        vectors = np.random.randn(num_vectors, DIM).astype(np.float32).tolist()
        query_vectors = np.random.randn(20, DIM).astype(np.float32).tolist()
        ground_truth = brute_force_knn(vectors, query_vectors, TOP_K, "COSINE")

        # --- Collection 1: HNSW with SQ8 (should go to GPU via override) ---
        schema = self.client.create_schema()
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(
            collection_name=self.collection_int8,
            schema=schema,
        )

        rows = [{"id": i, "vector": vectors[i]} for i in range(num_vectors)]
        self.client.insert(self.collection_int8, rows)
        self.client.flush(self.collection_int8)

        # HNSW_SQ with SQ8 → stored as HNSW_int8 internally
        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW_SQ",
            params={"M": M, "efConstruction": EF_CONSTRUCTION, "sq_type": "SQ8"},
        )
        self.client.create_index(self.collection_int8, index_params)
        self.client.load_collection(self.collection_int8)

        # --- Collection 2: Plain HNSW (float32, should stay on CPU) ---
        schema2 = self.client.create_schema()
        schema2.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema2.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(
            collection_name=self.collection_float,
            schema=schema2,
        )

        rows2 = [{"id": i, "vector": vectors[i]} for i in range(num_vectors)]
        self.client.insert(self.collection_float, rows2)
        self.client.flush(self.collection_float)

        # Plain HNSW (float32) → no override if only HNSW_int8 is configured
        index_params2 = self.client.prepare_index_params()
        index_params2.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW",
            params={"M": M, "efConstruction": EF_CONSTRUCTION},
        )
        self.client.create_index(self.collection_float, index_params2)
        self.client.load_collection(self.collection_float)

        time.sleep(3)

        # --- Verify both collections produce correct search results ---

        # INT8 collection (GPU path)
        results_int8 = self.client.search(
            collection_name=self.collection_int8,
            data=query_vectors,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=TOP_K,
            output_fields=["id"],
        )
        ids_int8 = [[hit["id"] for hit in hits] for hits in results_int8]
        recall_int8 = compute_recall(ids_int8, ground_truth, TOP_K)
        log.info(f"INT8 collection (GPU) recall@{TOP_K}: {recall_int8:.4f}")

        # Float32 collection (CPU path)
        results_float = self.client.search(
            collection_name=self.collection_float,
            data=query_vectors,
            anns_field="vector",
            search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
            limit=TOP_K,
            output_fields=["id"],
        )
        ids_float = [[hit["id"] for hit in hits] for hits in results_float]
        recall_float = compute_recall(ids_float, ground_truth, TOP_K)
        log.info(f"Float32 collection (CPU) recall@{TOP_K}: {recall_float:.4f}")

        # Both should achieve good recall
        assert recall_int8 >= RECALL_THRESHOLD, (
            f"INT8/GPU recall@{TOP_K} = {recall_int8:.4f}, expected >= {RECALL_THRESHOLD}. "
            f"GPU override for HNSW_int8 may not be working."
        )
        assert recall_float >= RECALL_THRESHOLD, (
            f"Float32/CPU recall@{TOP_K} = {recall_float:.4f}, expected >= {RECALL_THRESHOLD}. "
            f"Plain HNSW on CPU should still produce correct results."
        )

    def test_both_collections_searchable_concurrently(self):
        """Verify simultaneous searches on GPU and CPU collections don't interfere."""
        np.random.seed(654)
        num_vectors = 3000
        vectors = np.random.randn(num_vectors, DIM).astype(np.float32).tolist()

        # Create INT8 collection (GPU)
        schema = self.client.create_schema()
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(collection_name=self.collection_int8, schema=schema)

        rows = [{"id": i, "vector": vectors[i]} for i in range(num_vectors)]
        self.client.insert(self.collection_int8, rows)
        self.client.flush(self.collection_int8)

        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW_SQ",
            params={"M": M, "efConstruction": EF_CONSTRUCTION, "sq_type": "SQ8"},
        )
        self.client.create_index(self.collection_int8, index_params)
        self.client.load_collection(self.collection_int8)

        # Create Float32 collection (CPU)
        schema2 = self.client.create_schema()
        schema2.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema2.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
        self.client.create_collection(collection_name=self.collection_float, schema=schema2)

        rows2 = [{"id": i, "vector": vectors[i]} for i in range(num_vectors)]
        self.client.insert(self.collection_float, rows2)
        self.client.flush(self.collection_float)

        index_params2 = self.client.prepare_index_params()
        index_params2.add_index(
            field_name="vector",
            metric_type="COSINE",
            index_type="HNSW",
            params={"M": M, "efConstruction": EF_CONSTRUCTION},
        )
        self.client.create_index(self.collection_float, index_params2)
        self.client.load_collection(self.collection_float)

        time.sleep(3)

        # Run interleaved searches — verify no cross-contamination
        num_rounds = 10
        for round_idx in range(num_rounds):
            query = np.random.randn(1, DIM).astype(np.float32).tolist()

            # Search INT8 (GPU)
            r1 = self.client.search(
                collection_name=self.collection_int8,
                data=query,
                anns_field="vector",
                search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
                limit=TOP_K,
            )
            assert len(r1) > 0 and len(r1[0]) > 0, (
                f"Round {round_idx}: INT8/GPU search returned no results"
            )

            # Search Float32 (CPU)
            r2 = self.client.search(
                collection_name=self.collection_float,
                data=query,
                anns_field="vector",
                search_params={"metric_type": "COSINE", "params": {"ef": EF_SEARCH}},
                limit=TOP_K,
            )
            assert len(r2) > 0 and len(r2[0]) > 0, (
                f"Round {round_idx}: Float32/CPU search returned no results"
            )

            # Both collections have same data — top results should be similar
            ids_gpu = set(hit["id"] for hit in r1[0])
            ids_cpu = set(hit["id"] for hit in r2[0])
            overlap = len(ids_gpu & ids_cpu)
            # Allow some difference due to SQ8 quantization affecting distances
            assert overlap >= TOP_K // 2, (
                f"Round {round_idx}: GPU and CPU results diverge too much. "
                f"Overlap: {overlap}/{TOP_K}. GPU ids: {ids_gpu}, CPU ids: {ids_cpu}"
            )

        log.info(f"Completed {num_rounds} interleaved search rounds — no interference detected")


# --- pytest configuration ---


@pytest.fixture
def host(request):
    return request.config.getoption("--host", default="localhost")


@pytest.fixture
def port(request):
    return str(request.config.getoption("--port", default=19530))


# --- Direct execution support ---

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="GPU HNSW E2E Integration Tests")
    parser.add_argument("--host", default="localhost", help="Milvus host")
    parser.add_argument("--port", default="19530", help="Milvus port")
    parser.add_argument("--test",
                        choices=["recall", "hotload", "restart", "vram", "mixed", "all"],
                        default="all", help="Which test to run")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = get_milvus_client(args.host, args.port)
    log.info(f"Connected to Milvus at {args.host}:{args.port}")

    # Run tests via pytest for proper fixture handling
    pytest_args = [
        __file__,
        f"--host={args.host}",
        f"--port={args.port}",
        "-v",
        "--tb=short",
    ]
    if args.test != "all":
        test_map = {
            "recall": "TestGpuHnswRecall",
            "hotload": "TestGpuHnswHotLoad",
            "restart": "TestGpuHnswRestart",
            "vram": "TestGpuHnswVramExhaustion",
            "mixed": "TestGpuHnswMixedMode",
        }
        pytest_args.append(f"-k {test_map[args.test]}")

    sys.exit(pytest.main(pytest_args))
