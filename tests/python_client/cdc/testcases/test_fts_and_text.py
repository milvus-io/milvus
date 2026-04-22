"""
CDC sync tests for full-text search (BM25), text match, and phrase match operations.
"""

import random
import time

import pytest
from pymilvus import AnnSearchRequest, DataType, RRFRanker

from .base import TestCDCSyncBase, logger


class TestCDCSyncFTSAndText(TestCDCSyncBase):
    """Test CDC sync for full-text search, text match, and phrase match operations."""

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
    # Test 1: FTS insert and search replication
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("analyzer_type", ["standard", "english"])
    def test_fts_insert_and_search(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        analyzer_type,
    ):
        """Test FTS (BM25) insert and search replication.

        Creates an FTS collection with BM25 function, inserts 200 docs, creates
        SPARSE_INVERTED_INDEX (BM25) and HNSW indexes, then verifies that FTS search
        results replicate to downstream with sufficient overlap.
        """
        start_time = time.time()
        collection_name = self.gen_unique_name("fts_search", max_length=50)

        self.log_test_start(
            "test_fts_insert_and_search",
            f"FTS_INSERT_SEARCH(analyzer={analyzer_type})",
            collection_name,
        )

        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            # Initial cleanup
            self.cleanup_collection(upstream_client, collection_name)

            # Create FTS schema and collection
            logger.info(
                f"[CREATE] Creating FTS collection '{collection_name}' with analyzer_type='{analyzer_type}'"
            )
            schema = self.create_fts_schema(upstream_client, analyzer_type)
            upstream_client.create_collection(collection_name, schema=schema)

            # Insert 200 documents
            fts_data = self.generate_fts_data(200)
            logger.info(f"[INSERT] Inserting {len(fts_data)} FTS documents upstream")
            result = upstream_client.insert(collection_name, fts_data)
            inserted_count = result.get("insert_count", len(fts_data))
            logger.info(f"[INSERT] Inserted {inserted_count} documents")

            upstream_client.flush(collection_name)

            # Create SPARSE_INVERTED_INDEX on sparse_output (BM25)
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="sparse_output",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
                params={"bm25_k1": 1.5, "bm25_b": 0.75},
            )
            # Create HNSW on dense_vector
            index_params.add_index(
                field_name="dense_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Run FTS search on upstream
            query_text = "vector database similarity search"
            logger.info(f"[SEARCH] Running FTS search upstream with query: '{query_text}'")
            upstream_results = upstream_client.search(
                collection_name,
                data=[query_text],
                anns_field="sparse_output",
                limit=10,
                search_params={"metric_type": "BM25"},
                output_fields=["text_field", "category"],
            )
            upstream_ids = set()
            if upstream_results and len(upstream_results) > 0:
                for hit in upstream_results[0]:
                    upstream_ids.add(hit.get("id") or hit.id)

            logger.info(f"[SEARCH] Upstream FTS returned {len(upstream_ids)} results")

            # Wait for collection to appear downstream
            def check_collection_exists():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_collection_exists,
                sync_timeout,
                f"collection '{collection_name}' creation sync",
            ), f"Collection '{collection_name}' did not sync to downstream"

            # Wait for data + index to sync and downstream search results overlap
            def check_fts_overlap():
                try:
                    ds_results = downstream_client.search(
                        collection_name,
                        data=[query_text],
                        anns_field="sparse_output",
                        limit=10,
                        search_params={"metric_type": "BM25"},
                        output_fields=["text_field", "category"],
                    )
                    if not ds_results or len(ds_results) == 0:
                        return False
                    downstream_ids = set()
                    for hit in ds_results[0]:
                        downstream_ids.add(hit.get("id") or hit.id)
                    if not downstream_ids:
                        return False
                    if len(upstream_ids) == 0:
                        return len(downstream_ids) > 0
                    overlap = len(upstream_ids & downstream_ids) / max(
                        len(upstream_ids), 1
                    )
                    logger.info(
                        f"[OVERLAP] FTS search overlap: {overlap:.2f} "
                        f"(upstream={len(upstream_ids)}, downstream={len(downstream_ids)})"
                    )
                    return overlap >= self.SEARCH_OVERLAP_THRESHOLD
                except Exception as e:
                    logger.warning(f"FTS overlap check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_fts_overlap,
                sync_timeout,
                f"FTS search overlap sync (analyzer={analyzer_type})",
            ), (
                f"FTS search results did not reach overlap threshold "
                f"{self.SEARCH_OVERLAP_THRESHOLD} on downstream"
            )

            duration = time.time() - start_time
            self.log_test_end("test_fts_insert_and_search", True, duration)

        except Exception as exc:
            duration = time.time() - start_time
            self.log_test_end("test_fts_insert_and_search", False, duration)
            raise exc

    # -------------------------------------------------------------------------
    # Test 2: TEXT_MATCH sync
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("analyzer_type", ["standard", "english"])
    def test_text_match_sync(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        analyzer_type,
    ):
        """Test TEXT_MATCH query replication.

        Creates a collection with a VARCHAR field that has analyzer + match enabled,
        inserts 100 rows, and verifies that TEXT_MATCH queries return the same count
        on both upstream and downstream.
        """
        start_time = time.time()
        collection_name = self.gen_unique_name("text_match", max_length=50)

        self.log_test_start(
            "test_text_match_sync",
            f"TEXT_MATCH(analyzer={analyzer_type})",
            collection_name,
        )

        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Build schema with analyzer-enabled VARCHAR
            schema = upstream_client.create_schema(enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field("dense_vector", DataType.FLOAT_VECTOR, dim=128)
            schema.add_field(
                "text_field",
                DataType.VARCHAR,
                max_length=2048,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params={"type": analyzer_type},
            )

            logger.info(
                f"[CREATE] Creating text-match collection '{collection_name}'"
            )
            upstream_client.create_collection(collection_name, schema=schema)

            # Insert 100 rows from FTS_SENTENCES
            data = []
            for i in range(100):
                data.append(
                    {
                        "dense_vector": [random.random() for _ in range(128)],
                        "text_field": self.FTS_SENTENCES[i % len(self.FTS_SENTENCES)],
                    }
                )

            logger.info(f"[INSERT] Inserting {len(data)} rows upstream")
            upstream_client.insert(collection_name, data)
            upstream_client.flush(collection_name)

            # Create HNSW index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="dense_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Run TEXT_MATCH query on upstream
            filter_expr = "TEXT_MATCH(text_field, 'vector database')"
            logger.info(f"[QUERY] Upstream TEXT_MATCH query: {filter_expr}")
            upstream_results = upstream_client.query(
                collection_name,
                filter=filter_expr,
                output_fields=["id", "text_field"],
                limit=100,
            )
            upstream_count = len(upstream_results)
            logger.info(f"[QUERY] Upstream TEXT_MATCH returned {upstream_count} rows")

            # Wait for collection to appear downstream
            def check_collection_exists():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_collection_exists,
                sync_timeout,
                f"collection '{collection_name}' creation sync",
            )

            # Wait for downstream query count to match
            def check_count_match():
                try:
                    ds_results = downstream_client.query(
                        collection_name,
                        filter=filter_expr,
                        output_fields=["id", "text_field"],
                        limit=100,
                    )
                    ds_count = len(ds_results)
                    logger.info(
                        f"[VERIFY] TEXT_MATCH downstream count={ds_count}, "
                        f"upstream count={upstream_count}"
                    )
                    return ds_count == upstream_count
                except Exception as e:
                    logger.warning(f"TEXT_MATCH count check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_count_match,
                sync_timeout,
                f"TEXT_MATCH query count sync (analyzer={analyzer_type})",
            ), (
                f"TEXT_MATCH query count mismatch between upstream ({upstream_count}) "
                f"and downstream after timeout"
            )

            duration = time.time() - start_time
            self.log_test_end("test_text_match_sync", True, duration)

        except Exception as exc:
            duration = time.time() - start_time
            self.log_test_end("test_text_match_sync", False, duration)
            raise exc

    # -------------------------------------------------------------------------
    # Test 3: PHRASE_MATCH sync
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("analyzer_type", ["standard", "english"])
    def test_phrase_match_sync(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        analyzer_type,
    ):
        """Test PHRASE_MATCH query replication.

        Same collection setup as text_match.  Queries PHRASE_MATCH with slop=1 and
        verifies that upstream and downstream return the same count.
        """
        start_time = time.time()
        collection_name = self.gen_unique_name("phrase_match", max_length=50)

        self.log_test_start(
            "test_phrase_match_sync",
            f"PHRASE_MATCH(analyzer={analyzer_type})",
            collection_name,
        )

        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Build schema with analyzer-enabled VARCHAR
            schema = upstream_client.create_schema(enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field("dense_vector", DataType.FLOAT_VECTOR, dim=128)
            schema.add_field(
                "text_field",
                DataType.VARCHAR,
                max_length=2048,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params={"type": analyzer_type},
            )

            logger.info(
                f"[CREATE] Creating phrase-match collection '{collection_name}'"
            )
            upstream_client.create_collection(collection_name, schema=schema)

            # Insert 100 rows from FTS_SENTENCES
            data = []
            for i in range(100):
                data.append(
                    {
                        "dense_vector": [random.random() for _ in range(128)],
                        "text_field": self.FTS_SENTENCES[i % len(self.FTS_SENTENCES)],
                    }
                )

            logger.info(f"[INSERT] Inserting {len(data)} rows upstream")
            upstream_client.insert(collection_name, data)
            upstream_client.flush(collection_name)

            # Create HNSW index and load
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="dense_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Run PHRASE_MATCH query on upstream (slop=1)
            filter_expr = "PHRASE_MATCH(text_field, 'brown fox', 1)"
            logger.info(f"[QUERY] Upstream PHRASE_MATCH query: {filter_expr}")
            upstream_results = upstream_client.query(
                collection_name,
                filter=filter_expr,
                output_fields=["id", "text_field"],
                limit=100,
            )
            upstream_count = len(upstream_results)
            logger.info(
                f"[QUERY] Upstream PHRASE_MATCH returned {upstream_count} rows"
            )

            # Wait for collection to appear downstream
            def check_collection_exists():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_collection_exists,
                sync_timeout,
                f"collection '{collection_name}' creation sync",
            )

            # Wait for downstream query count to match
            def check_count_match():
                try:
                    ds_results = downstream_client.query(
                        collection_name,
                        filter=filter_expr,
                        output_fields=["id", "text_field"],
                        limit=100,
                    )
                    ds_count = len(ds_results)
                    logger.info(
                        f"[VERIFY] PHRASE_MATCH downstream count={ds_count}, "
                        f"upstream count={upstream_count}"
                    )
                    return ds_count == upstream_count
                except Exception as e:
                    logger.warning(f"PHRASE_MATCH count check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_count_match,
                sync_timeout,
                f"PHRASE_MATCH query count sync (analyzer={analyzer_type})",
            ), (
                f"PHRASE_MATCH query count mismatch between upstream ({upstream_count}) "
                f"and downstream after timeout"
            )

            duration = time.time() - start_time
            self.log_test_end("test_phrase_match_sync", True, duration)

        except Exception as exc:
            duration = time.time() - start_time
            self.log_test_end("test_phrase_match_sync", False, duration)
            raise exc

    # -------------------------------------------------------------------------
    # Test 4: Hybrid search (FTS + dense) replication
    # -------------------------------------------------------------------------

    def test_hybrid_search_fts_dense(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
    ):
        """Test hybrid search (BM25 sparse + HNSW dense) replication.

        Creates an FTS collection, inserts 300 docs, builds both sparse (BM25) and
        dense (HNSW) indexes, and verifies that hybrid search results on downstream
        have sufficient overlap with upstream results.
        """
        start_time = time.time()
        collection_name = self.gen_unique_name("hybrid_fts", max_length=50)

        self.log_test_start(
            "test_hybrid_search_fts_dense",
            "HYBRID_SEARCH_FTS_DENSE",
            collection_name,
        )

        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Create FTS schema (standard analyzer)
            schema = self.create_fts_schema(upstream_client, "standard")
            logger.info(f"[CREATE] Creating hybrid-search collection '{collection_name}'")
            upstream_client.create_collection(collection_name, schema=schema)

            # Insert 300 documents
            fts_data = self.generate_fts_data(300)
            logger.info(f"[INSERT] Inserting {len(fts_data)} documents upstream")
            upstream_client.insert(collection_name, fts_data)
            upstream_client.flush(collection_name)

            # Create both indexes
            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="sparse_output",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
                params={"bm25_k1": 1.5, "bm25_b": 0.75},
            )
            index_params.add_index(
                field_name="dense_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Build hybrid search requests
            sparse_req = AnnSearchRequest(
                data=["vector database similarity search"],
                anns_field="sparse_output",
                param={"metric_type": "BM25"},
                limit=10,
            )
            dense_query_vec = [random.random() for _ in range(128)]
            dense_req = AnnSearchRequest(
                data=[dense_query_vec],
                anns_field="dense_vector",
                param={"metric_type": "L2", "params": {"ef": 64}},
                limit=10,
            )

            logger.info("[SEARCH] Running hybrid search on upstream")
            upstream_results = upstream_client.hybrid_search(
                collection_name,
                reqs=[sparse_req, dense_req],
                ranker=RRFRanker(),
                limit=10,
                output_fields=["text_field", "category"],
            )

            upstream_ids = set()
            if upstream_results and len(upstream_results) > 0:
                for hit in upstream_results[0]:
                    upstream_ids.add(hit.get("id") or hit.id)

            logger.info(
                f"[SEARCH] Upstream hybrid search returned {len(upstream_ids)} results"
            )

            # Wait for collection to appear downstream
            def check_collection_exists():
                return downstream_client.has_collection(collection_name)

            assert self.wait_for_sync(
                check_collection_exists,
                sync_timeout,
                f"collection '{collection_name}' creation sync",
            )

            # Wait for downstream hybrid search overlap
            def check_hybrid_overlap():
                try:
                    ds_sparse_req = AnnSearchRequest(
                        data=["vector database similarity search"],
                        anns_field="sparse_output",
                        param={"metric_type": "BM25"},
                        limit=10,
                    )
                    ds_dense_req = AnnSearchRequest(
                        data=[dense_query_vec],
                        anns_field="dense_vector",
                        param={"metric_type": "L2", "params": {"ef": 64}},
                        limit=10,
                    )
                    ds_results = downstream_client.hybrid_search(
                        collection_name,
                        reqs=[ds_sparse_req, ds_dense_req],
                        ranker=RRFRanker(),
                        limit=10,
                        output_fields=["text_field", "category"],
                    )
                    if not ds_results or len(ds_results) == 0:
                        return False
                    downstream_ids = set()
                    for hit in ds_results[0]:
                        downstream_ids.add(hit.get("id") or hit.id)
                    if not downstream_ids:
                        return False
                    if len(upstream_ids) == 0:
                        return len(downstream_ids) > 0
                    overlap = len(upstream_ids & downstream_ids) / max(
                        len(upstream_ids), 1
                    )
                    logger.info(
                        f"[OVERLAP] Hybrid search overlap: {overlap:.2f} "
                        f"(upstream={len(upstream_ids)}, downstream={len(downstream_ids)})"
                    )
                    return overlap >= self.SEARCH_OVERLAP_THRESHOLD
                except Exception as e:
                    logger.warning(f"Hybrid overlap check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_hybrid_overlap,
                sync_timeout,
                "hybrid search (FTS + dense) overlap sync",
            ), (
                f"Hybrid search results did not reach overlap threshold "
                f"{self.SEARCH_OVERLAP_THRESHOLD} on downstream"
            )

            duration = time.time() - start_time
            self.log_test_end("test_hybrid_search_fts_dense", True, duration)

        except Exception as exc:
            duration = time.time() - start_time
            self.log_test_end("test_hybrid_search_fts_dense", False, duration)
            raise exc

    # -------------------------------------------------------------------------
    # Test 5: FTS after switchover
    # -------------------------------------------------------------------------

    def test_fts_after_switchover(
        self,
        upstream_client,
        downstream_client,
        sync_timeout,
        switchover_helper,
        source_cluster_id,
        target_cluster_id,
    ):
        """Test FTS replication continues correctly after CDC topology switchover.

        1. Create FTS collection, insert 100 docs, build index, verify sync.
        2. Perform switchover so downstream becomes the new source.
        3. Insert 50 more docs into the new source (original downstream).
        4. Verify FTS search works on the new downstream (original upstream).
        5. Switch back to original topology.
        """
        start_time = time.time()
        collection_name = self.gen_unique_name("fts_switchover", max_length=50)

        self.log_test_start(
            "test_fts_after_switchover",
            "FTS_AFTER_SWITCHOVER",
            collection_name,
        )

        self._upstream_client = upstream_client
        self.resources_to_cleanup.append(("collection", collection_name))

        try:
            self.cleanup_collection(upstream_client, collection_name)

            # Phase 1: Create FTS collection and index on original upstream
            logger.info(
                f"[PHASE1] Creating FTS collection '{collection_name}' on upstream"
            )
            schema = self.create_fts_schema(upstream_client, "standard")
            upstream_client.create_collection(collection_name, schema=schema)

            fts_data = self.generate_fts_data(100)
            logger.info(f"[PHASE1] Inserting {len(fts_data)} documents upstream")
            upstream_client.insert(collection_name, fts_data)
            upstream_client.flush(collection_name)

            index_params = upstream_client.prepare_index_params()
            index_params.add_index(
                field_name="sparse_output",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
                params={"bm25_k1": 1.5, "bm25_b": 0.75},
            )
            index_params.add_index(
                field_name="dense_vector",
                index_type="HNSW",
                metric_type="L2",
                params={"M": 8, "efConstruction": 64},
            )
            upstream_client.create_index(collection_name, index_params)
            upstream_client.load_collection(collection_name)

            # Verify initial sync to downstream
            def check_initial_sync():
                if not downstream_client.has_collection(collection_name):
                    return False
                try:
                    ds_results = downstream_client.search(
                        collection_name,
                        data=["vector database"],
                        anns_field="sparse_output",
                        limit=5,
                        search_params={"metric_type": "BM25"},
                        output_fields=["text_field"],
                    )
                    return ds_results is not None and len(ds_results) > 0
                except Exception:
                    return False

            assert self.wait_for_sync(
                check_initial_sync,
                sync_timeout,
                f"initial FTS sync for '{collection_name}'",
            ), f"Initial FTS sync failed for collection '{collection_name}'"

            logger.info("[PHASE1] Initial FTS sync verified")

            # Phase 2: Switchover — downstream becomes new source
            logger.info(
                f"[PHASE2] Switching CDC direction: {target_cluster_id} -> {source_cluster_id}"
            )
            switchover_helper(target_cluster_id, source_cluster_id)

            # Insert 50 more docs to the new source (original downstream)
            extra_data = self.generate_fts_data(50)
            logger.info(
                f"[PHASE2] Inserting {len(extra_data)} additional docs to new source (downstream_client)"
            )
            downstream_client.insert(collection_name, extra_data)
            downstream_client.flush(collection_name)

            # Phase 3: Verify FTS search works on new downstream (original upstream)
            query_text = "distributed database replication"

            def check_fts_on_new_downstream():
                try:
                    results = upstream_client.search(
                        collection_name,
                        data=[query_text],
                        anns_field="sparse_output",
                        limit=5,
                        search_params={"metric_type": "BM25"},
                        output_fields=["text_field"],
                    )
                    return results is not None and len(results) > 0
                except Exception as e:
                    logger.warning(f"FTS on new downstream check failed: {e}")
                    return False

            assert self.wait_for_sync(
                check_fts_on_new_downstream,
                sync_timeout,
                "FTS search on new downstream after switchover",
            ), "FTS search on new downstream (original upstream) failed after switchover"

            logger.info("[PHASE3] FTS search verified on new downstream after switchover")

            # Phase 4: Switch back to original topology
            logger.info(
                f"[PHASE4] Switching back to original topology: "
                f"{source_cluster_id} -> {target_cluster_id}"
            )
            switchover_helper(source_cluster_id, target_cluster_id)

            duration = time.time() - start_time
            self.log_test_end("test_fts_after_switchover", True, duration)

        except Exception as exc:
            # Best-effort restore original topology on failure
            try:
                logger.warning(
                    "[RECOVER] Attempting to restore original CDC topology after failure"
                )
                switchover_helper(source_cluster_id, target_cluster_id)
            except Exception as restore_exc:
                logger.error(f"[RECOVER] Failed to restore topology: {restore_exc}")
            duration = time.time() - start_time
            self.log_test_end("test_fts_after_switchover", False, duration)
            raise exc
