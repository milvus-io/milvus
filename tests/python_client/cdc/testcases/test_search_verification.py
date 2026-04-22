"""
CDC sync tests for search and query result verification across vector types.
"""

import random
import time

import pytest
from pymilvus import AnnSearchRequest, DataType, RRFRanker

from .base import TestCDCSyncBase, logger

# fmt: off
VECTOR_PARAMS = [
    ("FLOAT_VECTOR",        "HNSW",                  "COSINE",  128),
    ("FLOAT_VECTOR",        "IVF_FLAT",              "L2",      128),
    ("FLOAT16_VECTOR",      "HNSW",                  "L2",       64),
    ("BFLOAT16_VECTOR",     "HNSW",                  "L2",       64),
    ("INT8_VECTOR",         "HNSW",                  "COSINE",   64),
    ("BINARY_VECTOR",       "BIN_FLAT",              "HAMMING", 128),
    ("SPARSE_FLOAT_VECTOR", "SPARSE_INVERTED_INDEX", "IP",        0),
]
# fmt: on


class TestCDCSyncSearchVerification(TestCDCSyncBase):
    """Test CDC sync for search and query result verification across vector types."""

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
    # Internal helper
    # -------------------------------------------------------------------------

    def _setup_collection(self, client, c_name, vector_type, index_type, metric, dim):
        """
        Create a single-vector-schema collection, insert 500 records,
        create an index, and load.

        Returns the collection name (same as c_name).
        """
        schema = self.create_single_vector_schema(client, vector_type=vector_type, dim=dim)
        client.create_collection(collection_name=c_name, schema=schema)

        # Insert 500 records
        data = self.generate_single_vector_data(500, vector_type=vector_type, dim=dim)
        client.insert(c_name, data)
        client.flush(c_name)

        # Build index
        index_params = client.prepare_index_params()
        if index_type == "IVF_FLAT":
            idx_params = {"nlist": 64}
        elif index_type == "HNSW":
            idx_params = {"M": 16, "efConstruction": 200}
        else:
            idx_params = {}

        index_params.add_index(
            field_name="vector",
            index_type=index_type,
            metric_type=metric,
            params=idx_params,
        )
        client.create_index(c_name, index_params)
        client.load_collection(c_name)

        return c_name

    # -------------------------------------------------------------------------
    # Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize(
        "vector_type,index_type,metric,dim",
        VECTOR_PARAMS,
        ids=[p[0] + "_" + p[1] for p in VECTOR_PARAMS],
    )
    def test_search_result_consistency(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        vector_type,
        index_type,
        metric,
        dim,
    ):
        """Verify that ANN search results are consistent between upstream and downstream."""
        start_time = time.time()
        c_name = self.gen_unique_name(f"test_src_{vector_type[:4].lower()}", max_length=50)

        self.log_test_start(
            "test_search_result_consistency",
            f"SEARCH/{vector_type}/{index_type}",
            c_name,
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)
            self._setup_collection(upstream_client, c_name, vector_type, index_type, metric, dim)

            # Wait for at least 500 records to appear on downstream
            def check_sync():
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
                check_sync, sync_timeout, f"data sync 500 records {c_name}"
            ), f"Downstream did not receive 500 records within {sync_timeout}s"

            # Build 5 random query vectors
            dtype = getattr(DataType, vector_type)
            query_vectors = self._gen_vectors(5, dim if dim > 0 else 1000, dtype)

            avg_overlap, _, _ = self.verify_search_consistency(
                upstream_client,
                downstream_client,
                c_name,
                query_vectors,
                anns_field="vector",
                limit=10,
                metric_type=metric,
            )

            assert avg_overlap >= self.SEARCH_OVERLAP_THRESHOLD, (
                f"Search overlap {avg_overlap:.4f} is below threshold "
                f"{self.SEARCH_OVERLAP_THRESHOLD} for {vector_type}/{index_type}"
            )

        finally:
            self.log_test_end(
                "test_search_result_consistency",
                True,
                time.time() - start_time,
            )

    @pytest.mark.parametrize(
        "vector_type,index_type,metric,dim",
        VECTOR_PARAMS,
        ids=[p[0] + "_" + p[1] for p in VECTOR_PARAMS],
    )
    def test_query_data_sampling(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        vector_type,
        index_type,
        metric,
        dim,
    ):
        """Verify scalar field values are identical on both sides via random sampling."""
        start_time = time.time()
        c_name = self.gen_unique_name(f"test_qds_{vector_type[:4].lower()}", max_length=50)

        self.log_test_start(
            "test_query_data_sampling",
            f"QUERY_SAMPLE/{vector_type}/{index_type}",
            c_name,
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)
            self._setup_collection(upstream_client, c_name, vector_type, index_type, metric, dim)

            def check_sync():
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
                check_sync, sync_timeout, f"data sync 500 records {c_name}"
            ), f"Downstream did not receive 500 records within {sync_timeout}s"

            output_fields = ["id", "int_field", "varchar_field", "float_field"]
            match_count, mismatch_count, mismatch_details = self.verify_data_sampling(
                upstream_client,
                downstream_client,
                c_name,
                sample_ratio=0.2,
                output_fields=output_fields,
            )

            logger.info(
                f"[RESULT] Sampling — match={match_count}, mismatch={mismatch_count}, "
                f"details={mismatch_details[:3]}"
            )
            assert mismatch_count == 0, (
                f"Found {mismatch_count} mismatched records: {mismatch_details[:5]}"
            )

        finally:
            self.log_test_end(
                "test_query_data_sampling",
                True,
                time.time() - start_time,
            )

    def test_hybrid_search_consistency(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Verify hybrid search (dense + sparse, RRF ranker) results are consistent."""
        start_time = time.time()
        c_name = self.gen_unique_name("test_hybrid_srch", max_length=50)

        self.log_test_start("test_hybrid_search_consistency", "HYBRID_SEARCH", c_name)
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)

            # Build schema: dense FloatVector(128) + sparse
            schema = upstream_client.create_schema(enable_dynamic_field=True)
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field("dense", DataType.FLOAT_VECTOR, dim=128)
            schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
            schema.add_field("int_field", DataType.INT64)
            schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)

            upstream_client.create_collection(collection_name=c_name, schema=schema)

            # Insert 300 records
            dense_vecs = self._gen_vectors(300, 128, DataType.FLOAT_VECTOR)
            sparse_vecs = self._gen_vectors(300, 1000, DataType.SPARSE_FLOAT_VECTOR)
            data = [
                {
                    "dense": dense_vecs[i],
                    "sparse": sparse_vecs[i],
                    "int_field": random.randint(0, 1000),
                    "varchar_field": f"hybrid_{i}_{random.randint(1000, 9999)}",
                }
                for i in range(300)
            ]
            upstream_client.insert(c_name, data)
            upstream_client.flush(c_name)

            # Create indexes
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="dense",
                index_type="HNSW",
                metric_type="COSINE",
                params={"M": 16, "efConstruction": 200},
            )
            index_params.add_index(
                field_name="sparse",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
                params={},
            )
            upstream_client.create_index(c_name, index_params)
            upstream_client.load_collection(c_name)

            # Wait for downstream sync
            def check_sync():
                try:
                    res = downstream_client.query(
                        collection_name=c_name,
                        filter="",
                        output_fields=["count(*)"],
                    )
                    cnt = res[0]["count(*)"] if res else 0
                    logger.info(f"[SYNC_PROGRESS] downstream count: {cnt}/300")
                    return cnt >= 300
                except Exception as e:
                    logger.warning(f"Sync check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_sync, sync_timeout, f"hybrid data sync {c_name}"
            ), f"Downstream did not receive 300 records within {sync_timeout}s"

            # Build hybrid search requests
            q_dense = self._gen_vectors(1, 128, DataType.FLOAT_VECTOR)[0]
            q_sparse = self._gen_vectors(1, 1000, DataType.SPARSE_FLOAT_VECTOR)[0]

            dense_req = AnnSearchRequest(
                data=[q_dense],
                anns_field="dense",
                param={"metric_type": "COSINE", "params": {"ef": 64}},
                limit=10,
            )
            sparse_req = AnnSearchRequest(
                data=[q_sparse],
                anns_field="sparse",
                param={"metric_type": "IP"},
                limit=10,
            )

            up_results = upstream_client.hybrid_search(
                collection_name=c_name,
                reqs=[dense_req, sparse_req],
                ranker=RRFRanker(),
                limit=10,
                output_fields=["id"],
            )
            down_results = downstream_client.hybrid_search(
                collection_name=c_name,
                reqs=[dense_req, sparse_req],
                ranker=RRFRanker(),
                limit=10,
                output_fields=["id"],
            )

            up_pks = set(hit["id"] for hit in up_results[0]) if up_results else set()
            down_pks = set(hit["id"] for hit in down_results[0]) if down_results else set()
            union_size = len(up_pks | down_pks)
            overlap = len(up_pks & down_pks) / union_size if union_size > 0 else 1.0

            logger.info(
                f"[RESULT] Hybrid search PK overlap={overlap:.4f} "
                f"(up={len(up_pks)}, down={len(down_pks)})"
            )
            assert overlap >= self.SEARCH_OVERLAP_THRESHOLD, (
                f"Hybrid search overlap {overlap:.4f} below threshold "
                f"{self.SEARCH_OVERLAP_THRESHOLD}"
            )

        finally:
            self.log_test_end(
                "test_hybrid_search_consistency",
                True,
                time.time() - start_time,
            )

    def test_search_iterator_consistency(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Verify search iterator returns the same PK set on upstream and downstream."""
        start_time = time.time()
        c_name = self.gen_unique_name("test_srch_iter", max_length=50)

        self.log_test_start(
            "test_search_iterator_consistency", "SEARCH_ITERATOR", c_name
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)
            self._setup_collection(
                upstream_client, c_name, "FLOAT_VECTOR", "HNSW", "COSINE", 128
            )

            def check_sync():
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
                check_sync, sync_timeout, f"data sync 500 records {c_name}"
            ), f"Downstream did not receive 500 records within {sync_timeout}s"

            query_vec = self._gen_vectors(1, 128, DataType.FLOAT_VECTOR)[0]
            search_params = {"metric_type": "COSINE", "params": {"ef": 64}}

            def _collect_iterator_pks(client):
                pks = set()
                iterator = client.search_iterator(
                    collection_name=c_name,
                    data=[query_vec],
                    anns_field="vector",
                    batch_size=50,
                    limit=200,
                    param=search_params,
                    output_fields=["id"],
                )
                while True:
                    batch = iterator.next()
                    if not batch:
                        iterator.close()
                        break
                    for hit in batch:
                        pks.add(hit["id"])
                return pks

            up_pks = _collect_iterator_pks(upstream_client)
            down_pks = _collect_iterator_pks(downstream_client)
            union_size = len(up_pks | down_pks)
            overlap = len(up_pks & down_pks) / union_size if union_size > 0 else 1.0

            logger.info(
                f"[RESULT] Search iterator overlap={overlap:.4f} "
                f"(up={len(up_pks)}, down={len(down_pks)})"
            )
            assert overlap >= self.SEARCH_OVERLAP_THRESHOLD, (
                f"Search iterator PK overlap {overlap:.4f} below threshold "
                f"{self.SEARCH_OVERLAP_THRESHOLD}"
            )

        finally:
            self.log_test_end(
                "test_search_iterator_consistency",
                True,
                time.time() - start_time,
            )

    def test_query_iterator_consistency(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Verify that a query iterator retrieves identical PK sets from both sides."""
        start_time = time.time()
        c_name = self.gen_unique_name("test_qry_iter", max_length=50)

        self.log_test_start(
            "test_query_iterator_consistency", "QUERY_ITERATOR", c_name
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)
            self._setup_collection(
                upstream_client, c_name, "FLOAT_VECTOR", "HNSW", "COSINE", 128
            )

            def check_sync():
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
                check_sync, sync_timeout, f"data sync 500 records {c_name}"
            ), f"Downstream did not receive 500 records within {sync_timeout}s"

            up_count, down_count, match = self.verify_iterator_consistency(
                upstream_client,
                downstream_client,
                c_name,
                batch_size=100,
            )

            logger.info(
                f"[RESULT] Query iterator — upstream={up_count}, downstream={down_count}, match={match}"
            )
            assert match, (
                f"Query iterator PK sets differ: upstream={up_count}, downstream={down_count}"
            )

        finally:
            self.log_test_end(
                "test_query_iterator_consistency",
                True,
                time.time() - start_time,
            )

    def test_search_with_filter_consistency(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Verify filtered search produces consistent results honoring the filter predicate."""
        start_time = time.time()
        c_name = self.gen_unique_name("test_srch_filter", max_length=50)

        self.log_test_start(
            "test_search_with_filter_consistency", "SEARCH_WITH_FILTER", c_name
        )
        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", c_name))

        try:
            self.cleanup_collection(upstream_client, c_name)
            self._setup_collection(
                upstream_client, c_name, "FLOAT_VECTOR", "HNSW", "COSINE", 128
            )

            def check_sync():
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
                check_sync, sync_timeout, f"data sync 500 records {c_name}"
            ), f"Downstream did not receive 500 records within {sync_timeout}s"

            filter_expr = "int_field > 500"
            query_vec = self._gen_vectors(1, 128, DataType.FLOAT_VECTOR)[0]
            search_params = {"metric_type": "COSINE"}

            up_results = upstream_client.search(
                collection_name=c_name,
                data=[query_vec],
                anns_field="vector",
                search_params=search_params,
                filter=filter_expr,
                limit=10,
                output_fields=["id", "int_field"],
            )
            down_results = downstream_client.search(
                collection_name=c_name,
                data=[query_vec],
                anns_field="vector",
                search_params=search_params,
                filter=filter_expr,
                limit=10,
                output_fields=["id", "int_field"],
            )

            # Verify filter is honoured on both sides
            for hit in (up_results[0] if up_results else []):
                assert hit["int_field"] > 500, (
                    f"Filter violated on upstream: int_field={hit['int_field']}"
                )
            for hit in (down_results[0] if down_results else []):
                assert hit["int_field"] > 500, (
                    f"Filter violated on downstream: int_field={hit['int_field']}"
                )

            # Verify PK overlap
            up_pks = set(hit["id"] for hit in up_results[0]) if up_results else set()
            down_pks = set(hit["id"] for hit in down_results[0]) if down_results else set()
            union_size = len(up_pks | down_pks)
            overlap = len(up_pks & down_pks) / union_size if union_size > 0 else 1.0

            logger.info(
                f"[RESULT] Filtered search overlap={overlap:.4f} "
                f"(up={len(up_pks)}, down={len(down_pks)})"
            )
            assert overlap >= self.SEARCH_OVERLAP_THRESHOLD, (
                f"Filtered search overlap {overlap:.4f} below threshold "
                f"{self.SEARCH_OVERLAP_THRESHOLD}"
            )

        finally:
            self.log_test_end(
                "test_search_with_filter_consistency",
                True,
                time.time() - start_time,
            )
