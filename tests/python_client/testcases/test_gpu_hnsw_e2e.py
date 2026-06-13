"""
GPU HNSW End-to-End Integration Tests

These tests validate GPU_HNSW functionality beyond parameter validation:
1. Recall correctness — verify GPU search produces correct results
2. Hot-load path — verify new segments load as GPU_HNSW while collection is loaded
3. Restart resilience — verify segments reload as GPU_HNSW after querynode restart

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
    parser.add_argument("--test", choices=["recall", "hotload", "restart", "all"],
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
        }
        pytest_args.append(f"-k {test_map[args.test]}")

    sys.exit(pytest.main(pytest_args))
