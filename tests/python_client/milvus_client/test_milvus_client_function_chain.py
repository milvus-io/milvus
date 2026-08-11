import io
import json
import os
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import numpy as np
import pytest
import xgboost as xgb
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, FunctionChain, FunctionChainStage, RRFRanker
from pymilvus.client.types import LoadState
from pymilvus.function_chain import col, fn
from pymilvus.grpc_gen.common_pb2 import SegmentState
from utils.util_log import test_log as log


class TestMilvusClientFunctionChainXGBoost(TestMilvusClientV2Base):
    """MilvusClient coverage for XGBoost L0 FunctionChain reranking."""

    nb = ct.default_nb
    sealed_segment_count = 2
    segment_count = 3
    total_nb = nb * segment_count
    dim = ct.default_dim
    nq = ct.default_nq
    limit = ct.default_limit
    vector_field = "embedding"
    feature_names = [
        "user_level",
        "item_category_id",
        "recent_click_count",
        "inventory_count",
        "ctr_7d",
        "quality_score",
    ]
    feature_types = [
        DataType.INT8,
        DataType.INT16,
        DataType.INT32,
        DataType.INT64,
        DataType.FLOAT,
        DataType.DOUBLE,
    ]
    index_configs = {
        "FLAT": {
            "metric_type": "COSINE",
            "build_params": {},
            "search_params": {},
        },
        "HNSW": {
            "metric_type": "COSINE",
            "build_params": {"M": 16, "efConstruction": 200},
            "search_params": {"ef": 64},
        },
        "DISKANN": {
            "metric_type": "L2",
            "build_params": {},
            "search_params": {"search_list": 30},
        },
    }

    @staticmethod
    def _train_xgboost_model(
        tmp_path,
        name,
        features,
        labels,
        objective,
        num_boost_round=40,
    ):
        dtrain = xgb.DMatrix(
            np.asarray(features, dtype=np.float32),
            label=np.asarray(labels, dtype=np.float32),
        )
        booster = xgb.train(
            {
                "objective": objective,
                "booster": "gbtree",
                "max_depth": 6,
                "eta": 0.2,
                "lambda": 1.0,
                "alpha": 0.0,
                "base_score": 0.5,
                "tree_method": "hist",
                "seed": 19530,
                "nthread": 1,
            },
            dtrain,
            num_boost_round=num_boost_round,
        )
        model_path = tmp_path / f"{name}.ubj"
        booster.save_model(model_path)
        raw_scores = booster.predict(dtrain, output_margin=True).astype(float).tolist()
        default_scores = booster.predict(dtrain).astype(float).tolist()
        return booster, model_path, raw_scores, default_scores

    @staticmethod
    def _new_minio_client(minio_host):
        from minio import Minio

        return Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

    def _upload_model(self, client, minio_host, bucket, resource_name, remote_path, model_path):
        minio_client = self._new_minio_client(minio_host)
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
        model_data = model_path.read_bytes()
        minio_client.put_object(bucket, remote_path, io.BytesIO(model_data), len(model_data))
        self.add_file_resource(client, resource_name, remote_path)
        return minio_client

    @classmethod
    def _generate_recommendation_features(cls, seed, with_missing=False, row_count=None):
        row_count = cls.total_nb if row_count is None else row_count
        rng = np.random.default_rng(seed)
        columns = {
            "user_level": rng.integers(0, 8, row_count, dtype=np.int8),
            "item_category_id": rng.integers(0, 512, row_count, dtype=np.int16),
            "recent_click_count": rng.negative_binomial(8, 0.25, row_count).astype(np.int32),
            "inventory_count": np.clip(
                rng.lognormal(mean=9.0, sigma=1.1, size=row_count),
                0,
                5_000_000,
            ).astype(np.int64),
            "ctr_7d": rng.beta(2.2, 8.0, row_count).astype(np.float32),
            "quality_score": np.clip(
                rng.normal(loc=0.72, scale=0.14, size=row_count),
                0.0,
                1.0,
            ),
        }
        if with_missing:
            columns["ctr_7d"][rng.random(row_count) < 0.12] = np.nan
            columns["quality_score"][rng.random(row_count) < 0.08] = np.nan

        features = np.column_stack([columns[name] for name in cls.feature_names]).astype(np.float32)
        return columns, features

    @classmethod
    def _generate_embeddings(cls, seed, row_count=None):
        row_count = cls.total_nb if row_count is None else row_count
        rng = np.random.default_rng(seed)
        embeddings = rng.normal(size=(row_count, cls.dim)).astype(np.float32)
        embeddings /= np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings

    @classmethod
    def _generate_query_vectors(cls, seed, nq=None):
        nq = cls.nq if nq is None else nq
        rng = np.random.default_rng(seed)
        query_vectors = rng.normal(size=(nq, cls.dim)).astype(np.float32)
        query_vectors /= np.linalg.norm(query_vectors, axis=1, keepdims=True)
        return query_vectors.tolist()

    @classmethod
    def _build_rows(cls, columns, embeddings, id_start=1):
        rows = []
        for row_idx in range(len(embeddings)):
            row = {
                "id": id_start + row_idx,
                cls.vector_field: embeddings[row_idx].tolist(),
            }
            for feature_name in cls.feature_names:
                value = columns[feature_name][row_idx]
                if feature_name in {"ctr_7d", "quality_score"}:
                    row[feature_name] = None if np.isnan(value) else float(value)
                else:
                    row[feature_name] = int(value)
            rows.append(row)
        return rows

    def _create_empty_collection(self, client, nullable_features=False, index_type="FLAT"):
        collection_name = cf.gen_unique_str(f"xgboost_{index_type.lower()}_")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        for name, data_type in zip(self.feature_names, self.feature_types):
            schema.add_field(
                name,
                data_type,
                nullable=nullable_features and name in {"ctr_7d", "quality_score"},
            )
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)

        index_params = self.prepare_index_params(client)[0]
        index_config = self.index_configs[index_type]
        index_params.add_index(
            field_name=self.vector_field,
            index_type=index_type,
            metric_type=index_config["metric_type"],
            params=index_config["build_params"],
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        return collection_name

    def _create_collection(
        self,
        client,
        rows,
        nullable_features=False,
        index_type="FLAT",
        sealed_segment_count=None,
        replica_number=1,
    ):
        sealed_segment_count = self.sealed_segment_count if sealed_segment_count is None else sealed_segment_count
        total_nb = (sealed_segment_count + 1) * self.nb
        assert len(rows) == total_nb
        collection_name = self._create_empty_collection(
            client,
            nullable_features=nullable_features,
            index_type=index_type,
        )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"collection.autocompaction.enabled": "false"},
        )

        for batch_idx in range(sealed_segment_count):
            batch_start = batch_idx * self.nb
            self.insert(
                client,
                collection_name,
                rows[batch_start : batch_start + self.nb],
            )
            self.flush(client, collection_name)

        self.wait_for_index_ready(
            client,
            collection_name,
            index_name=self.vector_field,
        )
        # create_collection(schema + index_params) auto-loads one replica.  A
        # second load call with replica_number=2 exercises online replica
        # expansion and can leave the new replica waiting for outbound nodes.
        # Start the HA fixture from an unloaded collection so both replicas are
        # assigned atomically by the intended multi-replica load operation.
        if replica_number > 1:
            self.release_collection(client, collection_name)
        self.load_collection(
            client,
            collection_name,
            replica_number=replica_number,
            timeout=300 if replica_number > 1 else None,
        )

        loaded_segments = []
        deadline = time.time() + 180
        while time.time() < deadline:
            loaded_segments = client.list_loaded_segments(collection_name)
            if (
                len(loaded_segments) == sealed_segment_count
                and all(segment.state == SegmentState.Sealed for segment in loaded_segments)
                and sorted(segment.num_rows for segment in loaded_segments) == [self.nb] * sealed_segment_count
                and all(segment.index_id > 0 for segment in loaded_segments)
                and all(len(set(segment.node_ids)) == replica_number for segment in loaded_segments)
            ):
                break
            time.sleep(1)

        assert len(loaded_segments) == sealed_segment_count, loaded_segments
        assert all(segment.state == SegmentState.Sealed for segment in loaded_segments)
        assert sorted(segment.num_rows for segment in loaded_segments) == [self.nb] * sealed_segment_count
        assert all(segment.index_id > 0 for segment in loaded_segments)
        assert all(segment.index_name for segment in loaded_segments)
        assert all(len(set(segment.node_ids)) == replica_number for segment in loaded_segments)

        index_description = client.describe_index(
            collection_name,
            index_name=self.vector_field,
        )
        assert index_description["field_name"] == self.vector_field
        assert index_description["index_type"] == index_type
        assert index_description["pending_index_rows"] == 0

        persistent_segments = client.list_persistent_segments(collection_name)
        persistent_ids = {segment.segment_id for segment in persistent_segments}
        assert {segment.segment_id for segment in loaded_segments} <= persistent_ids
        sealed_ids = {segment.segment_id for segment in loaded_segments}

        self.insert(client, collection_name, rows[sealed_segment_count * self.nb :])
        growing_rows, _ = self.query(
            client,
            collection_name,
            filter=f"id > {sealed_segment_count * self.nb}",
            output_fields=["id"],
            limit=self.nb,
        )
        assert len(growing_rows) == self.nb
        assert {row["id"] for row in growing_rows} == set(
            range(
                sealed_segment_count * self.nb + 1,
                total_nb + 1,
            )
        )
        # The third batch is visible without a flush and does not create a new
        # persistent/loaded sealed segment during this setup window.
        loaded_after_growing = client.list_loaded_segments(collection_name)
        assert {segment.segment_id for segment in loaded_after_growing} == sealed_ids
        assert all(segment.state == SegmentState.Sealed for segment in loaded_after_growing)
        assert sorted(segment.num_rows for segment in loaded_after_growing) == [self.nb] * sealed_segment_count
        assert all(len(set(segment.node_ids)) == replica_number for segment in loaded_after_growing)
        persistent_after_growing = client.list_persistent_segments(collection_name)
        assert {segment.segment_id for segment in persistent_after_growing} == persistent_ids
        return collection_name

    @classmethod
    def _assert_multi_replica_topology(
        cls,
        client,
        collection_name,
        sealed_segment_count=2,
        replica_number=2,
        timeout=180,
    ):
        def topology_ready():
            replicas = client.describe_replica(collection_name)
            loaded_segments = client.list_loaded_segments(collection_name)
            resource_group = client.describe_resource_group("__default_resource_group")
            legacy_query_nodes = {node.node_id for node in resource_group.nodes}
            replica_nodes = {node_id for replica in replicas for node_id in replica.group_nodes}
            streaming_query_nodes = replica_nodes - legacy_query_nodes
            if len(replicas) != replica_number:
                return False
            if len(legacy_query_nodes) != replica_number:
                return False
            if len(streaming_query_nodes) != replica_number:
                return False
            if any(not replica.group_nodes for replica in replicas):
                return False
            if any(len(set(replica.group_nodes) & legacy_query_nodes) != 1 for replica in replicas):
                return False
            if any(len(set(replica.group_nodes) & streaming_query_nodes) != 1 for replica in replicas):
                return False
            if len(loaded_segments) != sealed_segment_count:
                return False
            if any(segment.state != SegmentState.Sealed for segment in loaded_segments):
                return False
            if sorted(segment.num_rows for segment in loaded_segments) != [cls.nb] * sealed_segment_count:
                return False
            if any(set(segment.node_ids) != legacy_query_nodes for segment in loaded_segments):
                return False
            return (
                replicas,
                loaded_segments,
                sorted(replica_nodes),
                sorted(legacy_query_nodes),
                sorted(streaming_query_nodes),
            )

        topology = cls._wait_until(
            topology_ready,
            timeout=timeout,
            interval=2,
        )
        assert topology, (
            f"collection {collection_name} did not reach {replica_number}-replica "
            f"fan-out for {sealed_segment_count} sealed segments"
        )
        (
            replicas,
            loaded_segments,
            replica_nodes,
            legacy_query_nodes,
            streaming_query_nodes,
        ) = topology
        return {
            "replica_ids": sorted(replica.id for replica in replicas),
            "replica_nodes": replica_nodes,
            "legacy_query_nodes": legacy_query_nodes,
            "streaming_query_nodes": streaming_query_nodes,
            "replica_groups": {str(replica.id): sorted(replica.group_nodes) for replica in replicas},
            "loaded_segments": {
                str(segment.segment_id): {
                    "num_rows": segment.num_rows,
                    "node_ids": sorted(segment.node_ids),
                    "index_id": segment.index_id,
                }
                for segment in loaded_segments
            },
        }

    @staticmethod
    def _xgboost_chain(resource_name, feature_names, output):
        return FunctionChain(FunctionChainStage.L0_RERANK, name="l0_xgboost").map(
            "$score",
            fn.xgboost(
                *(col(name) for name in feature_names),
                model_resource=resource_name,
                output=output,
            ),
        )

    def _search(
        self,
        client,
        collection_name,
        resource_name,
        feature_names,
        output,
        query_vectors,
        index_type="FLAT",
        search_param_overrides=None,
        limit=None,
        **kwargs,
    ):
        index_config = self.index_configs[index_type]
        params = dict(index_config["search_params"])
        if search_param_overrides:
            params.update(search_param_overrides)
        result, _ = self.search(
            client,
            collection_name,
            data=query_vectors,
            anns_field=self.vector_field,
            search_params={
                "metric_type": index_config["metric_type"],
                "params": params,
            },
            limit=self.limit if limit is None else limit,
            output_fields=feature_names,
            function_chains=self._xgboost_chain(resource_name, feature_names, output),
            **kwargs,
        )
        return result

    def _direct_xgboost_search(
        self,
        client,
        collection_name,
        resource_name,
        query_vectors,
        limit=None,
        feature_names=None,
        timeout=30,
        search_param_overrides=None,
    ):
        feature_names = self.feature_names if feature_names is None else feature_names
        params = dict(self.index_configs["HNSW"]["search_params"])
        if search_param_overrides:
            params.update(search_param_overrides)
        return client.search(
            collection_name=collection_name,
            data=query_vectors,
            anns_field=self.vector_field,
            search_params={"metric_type": "COSINE", "params": params},
            limit=self.limit if limit is None else limit,
            output_fields=feature_names,
            function_chains=self._xgboost_chain(
                resource_name,
                feature_names,
                "raw",
            ),
            timeout=timeout,
        )

    def _direct_plain_search(
        self,
        client,
        collection_name,
        query_vectors,
        limit=None,
        timeout=30,
        search_param_overrides=None,
    ):
        params = dict(self.index_configs["HNSW"]["search_params"])
        if search_param_overrides:
            params.update(search_param_overrides)
        return client.search(
            collection_name=collection_name,
            data=query_vectors,
            anns_field=self.vector_field,
            search_params={"metric_type": "COSINE", "params": params},
            limit=self.limit if limit is None else limit,
            output_fields=self.feature_names,
            timeout=timeout,
        )

    @staticmethod
    def _latency_summary(name, latencies):
        values = np.asarray(latencies, dtype=np.float64)
        assert values.size > 0
        total = float(values.sum())
        return {
            "name": name,
            "samples": int(values.size),
            "p50_ms": float(np.percentile(values, 50) * 1000),
            "p95_ms": float(np.percentile(values, 95) * 1000),
            "p99_ms": float(np.percentile(values, 99) * 1000),
            "max_ms": float(values.max() * 1000),
            "qps": float(values.size / total) if total > 0 else 0.0,
        }

    @staticmethod
    def _parse_duration_seconds(value):
        match = re.fullmatch(r"\s*(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?\s*", value or "")
        if not match or not any(match.groups()):
            raise ValueError(f"invalid request duration: {value!r}")
        hours, minutes, seconds = (int(part or 0) for part in match.groups())
        return hours * 3600 + minutes * 60 + seconds

    @staticmethod
    def _wait_until(probe, timeout, interval=1):
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                result = probe()
                if result:
                    return result
            except Exception as exc:
                last_error = exc
            threading.Event().wait(interval)
        if last_error is not None:
            log.warning("last polling error: %s", last_error)
        return False

    @staticmethod
    def _chaos_kube_env(request):
        kube_context = os.environ.get("MILVUS_KUBE_CONTEXT")
        release = os.environ.get("MILVUS_KUBE_RELEASE")
        if not kube_context or not release:
            pytest.skip("set MILVUS_KUBE_CONTEXT and MILVUS_KUBE_RELEASE for Chaos Mesh tests")
        return kube_context, request.config.getoption("--milvus_ns") or "chaos-testing", release

    @staticmethod
    def _kubectl_json(kube_context, namespace, *args):
        output = subprocess.check_output(
            ["kubectl", "--context", kube_context, "-n", namespace, *args, "-o", "json"],
            text=True,
        )
        return json.loads(output)

    @classmethod
    def _ready_pods(cls, kube_context, namespace, selector):
        pod_list = cls._kubectl_json(
            kube_context,
            namespace,
            "get",
            "pod",
            "-l",
            selector,
        )
        return sorted(
            [
                pod
                for pod in pod_list["items"]
                if not pod["metadata"].get("deletionTimestamp")
                and any(
                    condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in pod["status"].get("conditions", [])
                )
            ],
            key=lambda pod: pod["metadata"]["name"],
        )

    @classmethod
    def _ready_pod(cls, kube_context, namespace, selector):
        candidates = cls._ready_pods(kube_context, namespace, selector)
        return candidates[0] if len(candidates) == 1 else None

    @classmethod
    def _wait_ready_pods(cls, kube_context, namespace, selector, expected_count, timeout=180):
        return cls._wait_until(
            lambda: (
                pods if len(pods := cls._ready_pods(kube_context, namespace, selector)) == expected_count else False
            ),
            timeout=timeout,
            interval=2,
        )

    @classmethod
    def _wait_replacement_pod(cls, kube_context, namespace, selector, old_uid, timeout=180):
        return cls._wait_until(
            lambda: (
                pod
                if (pod := cls._ready_pod(kube_context, namespace, selector)) and pod["metadata"]["uid"] != old_uid
                else False
            ),
            timeout=timeout,
            interval=2,
        )

    @classmethod
    def _all_cluster_components_ready(cls, kube_context, namespace, release):
        pod_list = cls._kubectl_json(
            kube_context,
            namespace,
            "get",
            "pod",
            "-l",
            f"app.kubernetes.io/instance={release}",
        )
        expected = {"mixcoord", "proxy", "querynode", "datanode", "streamingnode"}
        ready = set()
        for pod in pod_list["items"]:
            if pod["metadata"].get("deletionTimestamp"):
                continue
            component = pod["metadata"].get("labels", {}).get("app.kubernetes.io/component")
            if component and any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod["status"].get("conditions", [])
            ):
                ready.add(component)
        return ready == expected

    @classmethod
    def _cluster_component_counts_ready(cls, kube_context, namespace, release, expected_counts):
        pod_list = cls._kubectl_json(
            kube_context,
            namespace,
            "get",
            "pod",
            "-l",
            f"app.kubernetes.io/instance={release}",
        )
        ready_counts = {component: 0 for component in expected_counts}
        for pod in pod_list["items"]:
            if pod["metadata"].get("deletionTimestamp"):
                continue
            component = pod["metadata"].get("labels", {}).get("app.kubernetes.io/component")
            if component not in ready_counts:
                continue
            if any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod["status"].get("conditions", [])
            ):
                ready_counts[component] += 1
        return ready_counts == expected_counts

    @staticmethod
    def _apply_chaos(kube_context, namespace, manifest):
        subprocess.run(
            ["kubectl", "--context", kube_context, "-n", namespace, "apply", "-f", "-"],
            input=json.dumps(manifest),
            text=True,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    @classmethod
    def _wait_chaos_injected(cls, kube_context, namespace, kind, name, timeout=60):
        def injected():
            resource = cls._kubectl_json(
                kube_context,
                namespace,
                "get",
                kind,
                name,
            )
            return any(
                condition.get("type") == "AllInjected" and condition.get("status") == "True"
                for condition in resource.get("status", {}).get("conditions", [])
            )

        assert cls._wait_until(injected, timeout=timeout, interval=1), f"{kind}/{name} was not injected"

    @staticmethod
    def _delete_chaos(kube_context, namespace, kind, name):
        subprocess.run(
            [
                "kubectl",
                "--context",
                kube_context,
                "-n",
                namespace,
                "delete",
                kind,
                name,
                "--ignore-not-found=true",
                "--wait=true",
                "--timeout=90s",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    @staticmethod
    def _pod_chaos_manifest(namespace, name, label_selectors):
        return {
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "PodChaos",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "action": "pod-kill",
                "mode": "one",
                "selector": {
                    "namespaces": [namespace],
                    "labelSelectors": label_selectors,
                },
                "gracePeriod": 0,
            },
        }

    @staticmethod
    def _network_partition_manifest(namespace, name, release):
        return {
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "action": "partition",
                "mode": "all",
                "selector": {
                    "namespaces": [namespace],
                    "labelSelectors": {
                        "app.kubernetes.io/instance": release,
                        "app.kubernetes.io/component": "querynode",
                    },
                },
                "direction": "to",
                "target": {
                    "mode": "all",
                    "selector": {
                        "namespaces": [namespace],
                        "labelSelectors": {
                            "app": "minio",
                            "release": f"{release}-minio",
                        },
                    },
                },
            },
        }

    @staticmethod
    def _log_non_functional_metrics(case_id, **metrics):
        payload = {
            "case_id": case_id,
            "run_id": os.environ.get("XGBOOST_NF_RUN_ID", "unscoped"),
            "utc_timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
            **metrics,
        }
        for env_name, payload_name in (
            ("XGBOOST_NF_ENDPOINT", "endpoint"),
            ("XGBOOST_NF_SERVER_IMAGE", "server_image"),
        ):
            if value := os.environ.get(env_name):
                payload[payload_name] = value
        line = json.dumps(payload, sort_keys=True)
        log.info("XGBOOST_NON_FUNCTIONAL_METRICS %s", line)
        metrics_path = os.environ.get("XGBOOST_NF_METRICS_FILE")
        if metrics_path:
            with open(metrics_path, "a", encoding="utf-8") as output:
                output.write(line + "\n")

    @classmethod
    def _assert_scores_match(
        cls,
        result_sets,
        expected_scores,
        expected_nq=None,
        expected_limit=None,
        score_sign=1.0,
    ):
        expected_by_id = {idx + 1: score for idx, score in enumerate(expected_scores)}
        assert len(result_sets) == (cls.nq if expected_nq is None else expected_nq)
        for hits in result_sets:
            hit_limit = cls.limit if expected_limit is None else expected_limit
            assert len(hits) == hit_limit
            assert len({hit["id"] for hit in hits}) == hit_limit
            for hit in hits:
                assert hit["id"] in expected_by_id
                assert hit["distance"] == pytest.approx(
                    score_sign * expected_by_id[hit["id"]],
                    rel=1e-5,
                    abs=1e-5,
                )
            actual_scores = [hit["distance"] for hit in hits]
            if score_sign > 0:
                assert all(left >= right - 1e-5 for left, right in zip(actual_scores, actual_scores[1:]))
            else:
                assert all(left <= right + 1e-5 for left, right in zip(actual_scores, actual_scores[1:]))

    @classmethod
    def _regression_dataset(cls, feature_seed, embedding_seed, query_seed, row_count=None):
        columns, features = cls._generate_recommendation_features(
            seed=feature_seed,
            row_count=row_count,
        )
        embeddings = cls._generate_embeddings(seed=embedding_seed, row_count=row_count)
        query_vectors = cls._generate_query_vectors(seed=query_seed)
        labels = (
            0.7 * columns["user_level"]
            + 0.08 * (columns["item_category_id"] % 17)
            + 0.9 * np.log1p(columns["recent_click_count"])
            + 0.35 * np.log1p(columns["inventory_count"])
            + 10.0 * columns["ctr_7d"]
            + 4.0 * columns["quality_score"]
            + 3.0 * (columns["user_level"] >= 5) * columns["ctr_7d"]
        )
        return columns, features, embeddings, query_vectors, labels

    @classmethod
    def _assert_rows_round_trip(cls, actual_rows, expected_rows):
        expected_by_id = {row["id"]: row for row in expected_rows}
        assert len(actual_rows) == len(expected_rows)
        assert {row["id"] for row in actual_rows} == set(expected_by_id)
        for actual in actual_rows:
            expected = expected_by_id[actual["id"]]
            for field in cls.feature_names:
                expected_value = expected[field]
                if expected_value is None:
                    assert actual[field] is None
                elif field in {"ctr_7d", "quality_score"}:
                    assert actual[field] == pytest.approx(
                        expected_value,
                        rel=1e-6,
                        abs=1e-6,
                    )
                else:
                    assert actual[field] == expected_value

    def _prepare_non_functional_case(
        self,
        client,
        tmp_path,
        minio_host,
        minio_bucket,
        case_name,
        seed,
        sealed_segment_count=None,
        num_boost_round=40,
        labels_transform=None,
        source_row_count=None,
        replica_number=1,
    ):
        sealed_segment_count = self.sealed_segment_count if sealed_segment_count is None else sealed_segment_count
        row_count = (sealed_segment_count + 1) * self.nb
        source_row_count = row_count if source_row_count is None else source_row_count
        assert source_row_count >= row_count
        columns, features, embeddings, _, labels = self._regression_dataset(
            feature_seed=seed,
            embedding_seed=seed + 1,
            query_seed=seed + 2,
            row_count=source_row_count,
        )
        if labels_transform is not None:
            labels = labels_transform(labels)
        _, model_path, expected_scores, _ = self._train_xgboost_model(
            tmp_path,
            case_name,
            features,
            labels,
            "reg:squarederror",
            num_boost_round=num_boost_round,
        )
        resource_name = cf.gen_unique_str(f"{case_name}_resource")
        remote_path = f"xgboost/{resource_name}.ubj"
        minio_client = self._upload_model(
            client,
            minio_host,
            minio_bucket,
            resource_name,
            remote_path,
            model_path,
        )
        try:
            collection_name = self._create_collection(
                client,
                self._build_rows(columns, embeddings)[:row_count],
                index_type="HNSW",
                sealed_segment_count=sealed_segment_count,
                replica_number=replica_number,
            )
        except Exception:
            self._cleanup_file_resource(
                client,
                minio_client,
                minio_bucket,
                resource_name,
                remote_path,
            )
            raise
        return {
            "collection_name": collection_name,
            "columns": columns,
            "features": features,
            "embeddings": embeddings,
            "labels": labels,
            "expected_scores": expected_scores,
            "resource_name": resource_name,
            "remote_path": remote_path,
            "model_path": model_path,
            "minio_client": minio_client,
            "row_count": row_count,
            "sealed_segment_count": sealed_segment_count,
            "replica_number": replica_number,
        }

    @staticmethod
    def _cleanup_file_resource(client, minio_client, minio_bucket, resource_name, remote_path):
        try:
            client.remove_file_resource(name=resource_name)
        except Exception as exc:
            log.warning("failed to remove FileResource %s during cleanup: %s", resource_name, exc)
        try:
            minio_client.remove_object(minio_bucket, remote_path)
        except Exception as exc:
            log.warning("failed to remove MinIO object %s during cleanup: %s", remote_path, exc)

    @pytest.mark.parametrize(
        "index_type",
        [
            pytest.param("FLAT", marks=pytest.mark.tags(CaseLabel.L1), id="flat"),
            pytest.param("HNSW", marks=pytest.mark.tags(CaseLabel.L0), id="hnsw"),
            pytest.param("DISKANN", marks=pytest.mark.tags(CaseLabel.L2), id="diskann"),
        ],
    )
    def test_milvus_client_xgboost_supports_all_numeric_feature_types(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
        index_type,
    ):
        """
        target: test numeric feature correctness in a realistic recommendation search
        method: train on 9000 distributed rows, create 2 sealed + 1 growing segment, then rerank FLAT/HNSW/DISKANN
        expected: all fields round-trip and correct/swapped feature orders match local predictions
        """
        client = self._client()
        resource_name = cf.gen_unique_str("xgboost_relevance_model")
        remote_path = f"xgboost/{resource_name}.ubj"
        columns, features, embeddings, query_vectors, labels = self._regression_dataset(
            feature_seed=19530,
            embedding_seed=19531,
            query_seed=19532,
        )
        booster, model_path, expected_scores, _ = self._train_xgboost_model(
            tmp_path,
            "xgboost_recommendation_relevance",
            features,
            labels,
            "reg:squarederror",
        )
        assert set(booster.get_score(importance_type="weight")) == {f"f{idx}" for idx in range(len(self.feature_names))}
        minio_client = self._upload_model(
            client,
            minio_host,
            minio_bucket,
            resource_name,
            remote_path,
            model_path,
        )

        try:
            rows = self._build_rows(columns, embeddings)
            collection_name = self._create_collection(client, rows, index_type=index_type)
            stored_rows, _ = self.query(
                client,
                collection_name,
                filter="id >= 1",
                output_fields=["id", *self.feature_names],
                limit=self.total_nb,
            )
            self._assert_rows_round_trip(stored_rows, rows)

            result_sets = self._search(
                client,
                collection_name,
                resource_name,
                self.feature_names,
                output="raw",
                query_vectors=query_vectors,
                index_type=index_type,
            )
            score_sign = -1.0 if self.index_configs[index_type]["metric_type"] == "L2" else 1.0
            self._assert_scores_match(
                result_sets,
                expected_scores,
                score_sign=score_sign,
            )

            swapped_feature_names = self.feature_names.copy()
            swapped_feature_names[0], swapped_feature_names[1] = (
                swapped_feature_names[1],
                swapped_feature_names[0],
            )
            swapped_features = features[:, [1, 0, 2, 3, 4, 5]]
            swapped_expected = booster.predict(xgb.DMatrix(swapped_features), output_margin=True).astype(float).tolist()
            changed_predictions = np.count_nonzero(
                np.abs(np.asarray(expected_scores) - np.asarray(swapped_expected)) > 1e-5
            )
            assert changed_predictions >= int(self.total_nb * 0.9)
            swapped_result_sets = self._search(
                client,
                collection_name,
                resource_name,
                swapped_feature_names,
                output="raw",
                query_vectors=query_vectors,
                index_type=index_type,
            )
            self._assert_scores_match(
                swapped_result_sets,
                swapped_expected,
                score_sign=score_sign,
            )
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(minio_bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_xgboost_multiple_sealed_and_growing_segments(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: test XGBoost L0 rerank across multiple sealed and growing segments
        method: create two indexed 3000-row sealed segments, then insert one unflushed 3000-row growing segment
        expected: segment topology is proven and per-segment/mixed search scores match the local model
        """
        client = self._client()
        resource_name = cf.gen_unique_str("xgboost_multi_segment_model")
        remote_path = f"xgboost/{resource_name}.ubj"

        columns, features, embeddings, query_vectors, labels = self._regression_dataset(
            feature_seed=19540,
            embedding_seed=19541,
            query_seed=19542,
        )
        rows = self._build_rows(columns, embeddings)

        booster, model_path, expected_scores, _ = self._train_xgboost_model(
            tmp_path,
            "xgboost_multi_segment_relevance",
            features,
            labels,
            "reg:squarederror",
        )
        assert set(booster.get_score(importance_type="weight")) == {f"f{idx}" for idx in range(len(self.feature_names))}
        minio_client = self._upload_model(
            client,
            minio_host,
            minio_bucket,
            resource_name,
            remote_path,
            model_path,
        )

        try:
            collection_name = self._create_collection(
                client,
                rows,
                index_type="HNSW",
            )

            id_ranges = {
                "sealed_1": (1, self.nb),
                "sealed_2": (self.nb + 1, 2 * self.nb),
                "growing": (2 * self.nb + 1, 3 * self.nb),
            }
            for lower, upper in id_ranges.values():
                result_sets = self._search(
                    client,
                    collection_name,
                    resource_name,
                    self.feature_names,
                    output="raw",
                    query_vectors=query_vectors,
                    index_type="HNSW",
                    filter=f"id >= {lower} and id <= {upper}",
                )
                self._assert_scores_match(result_sets, expected_scores)
                assert all(lower <= hit["id"] <= upper for hits in result_sets for hit in hits)

            mixed_limit = self.limit * 3
            mixed_results = self._search(
                client,
                collection_name,
                resource_name,
                self.feature_names,
                output="raw",
                query_vectors=query_vectors,
                index_type="HNSW",
                limit=mixed_limit,
            )
            self._assert_scores_match(
                mixed_results,
                expected_scores,
                expected_limit=mixed_limit,
            )
            for hits in mixed_results:
                sources = {
                    "sealed_1" if hit["id"] <= self.nb else "sealed_2" if hit["id"] <= 2 * self.nb else "growing"
                    for hit in hits
                }
                assert sources == set(id_ranges), sources
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(minio_bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_xgboost_delete_and_upsert_data_correctness(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify XGB-DATA-003/004 correctness after delete and upsert
        method: mutate disjoint PKs from both 3000-row sealed segments and the 3000-row growing segment
        expected: deletes disappear and upserts expose only new fields, vectors, and XGBoost scores
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_mutation_correctness",
            19550,
        )
        rows = self._build_rows(fixture["columns"], fixture["embeddings"])
        query_vector = self._generate_query_vectors(19553, nq=1)
        delete_ids = [
            *range(101, 111),
            *range(self.nb + 101, self.nb + 111),
            *range(2 * self.nb + 101, 2 * self.nb + 111),
        ]
        control_ids = [
            *range(201, 211),
            *range(self.nb + 201, self.nb + 211),
            *range(2 * self.nb + 201, 2 * self.nb + 211),
        ]
        delete_filter = f"id in {delete_ids}"
        control_filter = f"id in {control_ids}"

        booster = xgb.Booster()
        booster.load_model(fixture["model_path"])
        mutated_columns = {
            "user_level": (7 - fixture["columns"]["user_level"]).astype(np.int8),
            "item_category_id": ((fixture["columns"]["item_category_id"] + 257) % 512).astype(np.int16),
            "recent_click_count": (fixture["columns"]["recent_click_count"] + 127).astype(np.int32),
            "inventory_count": (fixture["columns"]["inventory_count"] // 2 + 1_234_567).astype(np.int64),
            "ctr_7d": (1.0 - fixture["columns"]["ctr_7d"]).astype(np.float32),
            "quality_score": (1.0 - fixture["columns"]["quality_score"]).astype(np.float64),
        }
        mutated_features = np.column_stack([mutated_columns[name] for name in self.feature_names]).astype(np.float32)
        mutated_scores = booster.predict(
            xgb.DMatrix(mutated_features),
            output_margin=True,
        ).astype(float)
        original_scores = np.asarray(fixture["expected_scores"], dtype=np.float64)

        upsert_ids = []
        for segment_start in (0, self.nb, 2 * self.nb):
            eligible_ids = [
                entity_id
                for entity_id in range(segment_start + 401, segment_start + 801)
                if abs(mutated_scores[entity_id - 1] - original_scores[entity_id - 1]) > 1e-4
            ]
            assert len(eligible_ids) >= 10
            upsert_ids.extend(eligible_ids[:10])
        assert set(delete_ids).isdisjoint(control_ids)
        assert set(delete_ids).isdisjoint(upsert_ids)
        assert set(control_ids).isdisjoint(upsert_ids)

        upsert_indices = np.asarray(upsert_ids, dtype=np.int64) - 1
        upsert_columns = {name: values[upsert_indices] for name, values in mutated_columns.items()}
        upsert_embeddings = -fixture["embeddings"][upsert_indices]
        upsert_rows = self._build_rows(upsert_columns, upsert_embeddings)
        for entity_id, row in zip(upsert_ids, upsert_rows):
            row["id"] = entity_id
        upsert_by_id = {row["id"]: row for row in upsert_rows}

        try:
            delete_result = self.delete(
                client,
                fixture["collection_name"],
                filter=delete_filter,
            )[0]
            assert delete_result["delete_count"] == len(delete_ids)

            deleted_rows = self.query(
                client,
                fixture["collection_name"],
                filter=delete_filter,
                output_fields=["id", *self.feature_names],
                limit=len(delete_ids),
            )[0]
            assert deleted_rows == []

            plain_deleted = self.search(
                client,
                fixture["collection_name"],
                data=query_vector,
                anns_field=self.vector_field,
                search_params={
                    "metric_type": "COSINE",
                    "params": self.index_configs["HNSW"]["search_params"],
                },
                limit=self.limit,
                filter=delete_filter,
            )[0]
            assert len(plain_deleted) == 1
            assert plain_deleted[0] == []

            reranked_deleted = self._search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                self.feature_names,
                output="raw",
                query_vectors=query_vector,
                index_type="HNSW",
                limit=self.limit,
                filter=delete_filter,
            )
            assert len(reranked_deleted) == 1
            assert reranked_deleted[0] == []

            control_results = self._search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                self.feature_names,
                output="raw",
                query_vectors=query_vector,
                index_type="HNSW",
                limit=len(control_ids),
                filter=control_filter,
            )
            self._assert_scores_match(
                control_results,
                fixture["expected_scores"],
                expected_nq=1,
                expected_limit=len(control_ids),
            )
            assert {hit["id"] for hit in control_results[0]} == set(control_ids)
            self._assert_rows_round_trip(
                control_results[0],
                [rows[entity_id - 1] for entity_id in control_ids],
            )

            count_after_delete = self.query(
                client,
                fixture["collection_name"],
                filter="",
                output_fields=["count(*)"],
            )[0]
            assert count_after_delete == [{"count(*)": self.total_nb - len(delete_ids)}]

            upsert_result = self.upsert(
                client,
                fixture["collection_name"],
                upsert_rows,
            )[0]
            assert upsert_result["upsert_count"] == len(upsert_ids)

            stored_upserts = self.query(
                client,
                fixture["collection_name"],
                filter=f"id in {upsert_ids}",
                output_fields=["id", *self.feature_names, self.vector_field],
                limit=len(upsert_ids),
            )[0]
            self._assert_rows_round_trip(stored_upserts, upsert_rows)
            assert len({row["id"] for row in stored_upserts}) == len(upsert_ids)
            for actual in stored_upserts:
                np.testing.assert_allclose(
                    actual[self.vector_field],
                    upsert_by_id[actual["id"]][self.vector_field],
                    rtol=1e-6,
                    atol=1e-6,
                )

            plain_upserts = self.search(
                client,
                fixture["collection_name"],
                data=upsert_embeddings.tolist(),
                anns_field=self.vector_field,
                search_params={
                    "metric_type": "COSINE",
                    "params": self.index_configs["HNSW"]["search_params"],
                },
                limit=1,
                filter=f"id in {upsert_ids}",
                output_fields=self.feature_names,
            )[0]
            assert len(plain_upserts) == len(upsert_ids)
            for entity_id, hits in zip(upsert_ids, plain_upserts):
                assert len(hits) == 1
                assert hits[0]["id"] == entity_id
                assert hits[0]["distance"] == pytest.approx(1.0, rel=1e-5, abs=1e-5)

            expected_after_upsert = fixture["expected_scores"].copy()
            for entity_id in upsert_ids:
                expected_after_upsert[entity_id - 1] = float(mutated_scores[entity_id - 1])
            upsert_results = self._search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                self.feature_names,
                output="raw",
                query_vectors=self._generate_query_vectors(19554),
                index_type="HNSW",
                limit=len(upsert_ids),
                filter=f"id in {upsert_ids}",
            )
            self._assert_scores_match(
                upsert_results,
                expected_after_upsert,
                expected_limit=len(upsert_ids),
            )
            for hits in upsert_results:
                assert {hit["id"] for hit in hits} == set(upsert_ids)
                self._assert_rows_round_trip(hits, upsert_rows)
                for hit in hits:
                    assert abs(hit["distance"] - original_scores[hit["id"] - 1]) > 1e-4

            count_after_upsert = self.query(
                client,
                fixture["collection_name"],
                filter="",
                output_fields=["count(*)"],
            )[0]
            assert count_after_upsert == count_after_delete
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.parametrize(
        "search_mode",
        [
            pytest.param("filtered", marks=pytest.mark.tags(CaseLabel.L1), id="filtered"),
            pytest.param("range", marks=pytest.mark.tags(CaseLabel.L1), id="range"),
            pytest.param("group_by", marks=pytest.mark.tags(CaseLabel.L1), id="group-by"),
            pytest.param("iterator", marks=pytest.mark.tags(CaseLabel.L2), id="iterator"),
        ],
    )
    def test_milvus_client_xgboost_search_modes(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
        search_mode,
    ):
        """
        target: test XGBoost rerank across ordinary SearchRequest modes
        method: run HNSW filtered/range/group-by/iterator search on 2 sealed + 1 growing segment
        expected: supported mode scores match locally; iterator v2 + function rerank is explicitly rejected
        """
        client = self._client()
        resource_name = cf.gen_unique_str(f"xgboost_{search_mode}_model")
        remote_path = f"xgboost/{resource_name}.ubj"
        columns, features, embeddings, query_vectors, labels = self._regression_dataset(
            feature_seed=19536,
            embedding_seed=19537,
            query_seed=19538,
        )
        booster, model_path, expected_scores, _ = self._train_xgboost_model(
            tmp_path,
            f"xgboost_{search_mode}_relevance",
            features,
            labels,
            "reg:squarederror",
        )
        assert set(booster.get_score(importance_type="weight")) == {f"f{idx}" for idx in range(len(self.feature_names))}
        minio_client = self._upload_model(
            client,
            minio_host,
            minio_bucket,
            resource_name,
            remote_path,
            model_path,
        )

        try:
            rows = self._build_rows(columns, embeddings)
            collection_name = self._create_collection(client, rows, index_type="HNSW")

            if search_mode == "iterator":

                def read_two_control_pages():
                    iterator, _ = self.search_iterator(
                        client,
                        collection_name,
                        data=[query_vectors[0]],
                        batch_size=self.limit,
                        limit=self.limit * 2,
                        output_fields=self.feature_names,
                        search_params={
                            "metric_type": "COSINE",
                            "params": self.index_configs["HNSW"]["search_params"],
                        },
                        anns_field=self.vector_field,
                    )
                    try:
                        return [list(iterator.next()), list(iterator.next())]
                    finally:
                        iterator.close()

                control_pages = read_two_control_pages()
                control_first_ids = {hit["id"] for hit in control_pages[0]}
                control_second_ids = {hit["id"] for hit in control_pages[1]}
                assert control_first_ids.isdisjoint(control_second_ids)

                self.search_iterator(
                    client,
                    collection_name,
                    data=[query_vectors[0]],
                    batch_size=self.limit,
                    limit=self.limit * 2,
                    output_fields=self.feature_names,
                    search_params={
                        "metric_type": "COSINE",
                        "params": self.index_configs["HNSW"]["search_params"],
                    },
                    anns_field=self.vector_field,
                    function_chains=self._xgboost_chain(
                        resource_name,
                        self.feature_names,
                        "raw",
                    ),
                    check_task=CheckTasks.err_res,
                    check_items={
                        ct.err_code: 1100,
                        ct.err_msg: "function rerank is not supported with search iterator v2",
                    },
                )
                return

            search_kwargs = {}
            search_param_overrides = None
            if search_mode == "filtered":
                search_kwargs["filter"] = "inventory_count >= 10000 and ctr_7d >= 0.1"
            elif search_mode == "range":
                search_param_overrides = {"radius": 0.0, "range_filter": 0.5}
            elif search_mode == "group_by":
                search_kwargs["group_by_field"] = "item_category_id"

            result_sets = self._search(
                client,
                collection_name,
                resource_name,
                self.feature_names,
                output="raw",
                query_vectors=query_vectors,
                index_type="HNSW",
                search_param_overrides=search_param_overrides,
                **search_kwargs,
            )
            self._assert_scores_match(result_sets, expected_scores)

            if search_mode == "filtered":
                expected_by_id = {row["id"]: row for row in rows}
                assert all(
                    expected_by_id[hit["id"]]["inventory_count"] >= 10000 and expected_by_id[hit["id"]]["ctr_7d"] >= 0.1
                    for hits in result_sets
                    for hit in hits
                )
            elif search_mode == "range":
                for query_vector, hits in zip(query_vectors, result_sets):
                    for hit in hits:
                        cosine = float(np.dot(query_vector, embeddings[hit["id"] - 1]))
                        assert 0.0 < cosine <= 0.5 + 1e-6
            elif search_mode == "group_by":
                for hits in result_sets:
                    categories = [hit.get("item_category_id") for hit in hits]
                    assert len(categories) == len(set(categories)) == self.limit
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(minio_bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_xgboost_hybrid_search_rejected(self):
        """
        target: test the explicit hybrid-search compatibility boundary
        method: pass an XGBoost FunctionChain with two valid AnnSearchRequests
        expected: PyMilvus rejects FunctionChain instead of silently ignoring it
        """
        request = AnnSearchRequest(
            data=[[0.0] * self.dim],
            anns_field=self.vector_field,
            param={"metric_type": "COSINE"},
            limit=self.limit,
        )
        self.hybrid_search(
            self._client(),
            collection_name=cf.gen_unique_str("xgboost_hybrid_boundary_"),
            reqs=[request, request],
            ranker=RRFRanker(),
            limit=self.limit,
            function_chains=self._xgboost_chain(
                "unused_xgboost_resource",
                self.feature_names,
                "raw",
            ),
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1,
                ct.err_msg: "function_chains is not supported for hybrid_search yet",
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_xgboost_rejects_l2_rerank_stage(self):
        """
        target: verify XGBoost is restricted to the L0 FunctionChain stage
        method: run an L2 XGBoost map against the standard 2 sealed + 1 growing HNSW topology
        expected: server rejects the function/stage combination before resolving the model resource
        """
        client = self._client()
        columns, _, embeddings, query_vectors, _ = self._regression_dataset(
            feature_seed=19542,
            embedding_seed=19543,
            query_seed=19544,
        )
        collection_name = self._create_collection(
            client,
            self._build_rows(columns, embeddings),
            index_type="HNSW",
        )
        l2_chain = FunctionChain(FunctionChainStage.L2_RERANK, name="invalid_l2_xgboost").map(
            "$score",
            fn.xgboost(
                *(col(name) for name in self.feature_names),
                model_resource="unused_xgboost_resource",
                output="raw",
            ),
        )

        self.search(
            client,
            collection_name,
            data=query_vectors,
            anns_field=self.vector_field,
            search_params={
                "metric_type": "COSINE",
                "params": self.index_configs["HNSW"]["search_params"],
            },
            limit=self.limit,
            output_fields=self.feature_names,
            function_chains=l2_chain,
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: 'function "xgboost" does not support stage "L2_rerank"',
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_xgboost_rejects_corrupted_ubj_model(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: reject a registered .ubj whose bytes are a truncated real XGBoost model
        method: search the 3x3000 HNSW topology with the corrupted resource, then retry with the valid model
        expected: model load fails explicitly without ANN fallback and the valid resource still reranks correctly
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_corrupted_ubj_control",
            19545,
        )
        query_vectors = self._generate_query_vectors(19548)
        corrupted_resource = cf.gen_unique_str("xgboost_corrupted_ubj")
        corrupted_path = f"xgboost/{corrupted_resource}.ubj"
        valid_bytes = fixture["model_path"].read_bytes()
        truncated_bytes = valid_bytes[: max(64, len(valid_bytes) // 2)]
        assert 0 < len(truncated_bytes) < len(valid_bytes)
        fixture["minio_client"].put_object(
            minio_bucket,
            corrupted_path,
            io.BytesIO(truncated_bytes),
            len(truncated_bytes),
        )
        self.add_file_resource(client, corrupted_resource, corrupted_path)

        try:
            with pytest.raises(Exception) as exc_info:
                self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    corrupted_resource,
                    query_vectors,
                )
            error_code = getattr(exc_info.value, "code", None)
            error_is_input = getattr(exc_info.value, "is_input_error", None)
            error_retriable = getattr(exc_info.value, "retriable", None)
            error_text = str(exc_info.value)
            error_is_explicit = re.search(
                r"(?i)(xgboost|ubj).*(parse|load|model|buffer|offset)|"
                r"(parse|load).*(xgboost|ubj|model)",
                error_text,
            )

            recovered = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
            )
            self._assert_scores_match(recovered, fixture["expected_scores"])
            assert error_is_explicit, error_text
            assert error_code == 2000, f"unexpected wire error code {error_code}: {error_text}"
            assert error_is_input is True, f"corrupted user model was not classified as input error: {error_text}"
            assert error_retriable is False, f"corrupted user model was unexpectedly retriable: {error_text}"
            assert "segcoreCode=2042" in error_text, f"precise segcore error code was not preserved: {error_text}"
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                corrupted_resource,
                corrupted_path,
            )
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_xgboost_binary_outputs_and_null_features(self, tmp_path, minio_host, minio_bucket):
        """
        target: test nullable features and binary outputs in a realistic click model
        method: train 9000 rows with distributed NULLs and search 2 sealed + 1 growing segment
        expected: NULLs round-trip and both output modes match local predictions for 2-query topK
        """
        client = self._client()
        resource_name = cf.gen_unique_str("xgboost_click_model")
        remote_path = f"xgboost/{resource_name}.ubj"
        columns, features = self._generate_recommendation_features(
            seed=19533,
            with_missing=True,
        )
        embeddings = self._generate_embeddings(seed=19534)
        query_vectors = self._generate_query_vectors(seed=19535)
        logits = (
            0.5 * columns["user_level"]
            + 0.05 * (columns["item_category_id"] % 17)
            + 0.5 * np.log1p(columns["recent_click_count"])
            + 0.15 * np.log1p(columns["inventory_count"])
            + 5.0 * np.nan_to_num(columns["ctr_7d"], nan=-0.2)
            + 2.0 * np.nan_to_num(columns["quality_score"], nan=0.2)
        )
        labels = (logits > np.median(logits)).astype(np.float32)
        booster, model_path, expected_raw, expected_default = self._train_xgboost_model(
            tmp_path,
            "xgboost_recommendation_click",
            features,
            labels,
            "binary:logistic",
        )
        assert set(booster.get_score(importance_type="weight")) == {f"f{idx}" for idx in range(len(self.feature_names))}
        assert np.count_nonzero(np.isnan(columns["ctr_7d"])) >= int(self.total_nb * 0.10)
        assert np.count_nonzero(np.isnan(columns["quality_score"])) >= int(self.total_nb * 0.06)
        assert 0.45 <= labels.mean() <= 0.55
        minio_client = self._upload_model(
            client,
            minio_host,
            minio_bucket,
            resource_name,
            remote_path,
            model_path,
        )

        try:
            rows = self._build_rows(columns, embeddings)
            collection_name = self._create_collection(
                client,
                rows,
                nullable_features=True,
            )
            stored_rows, _ = self.query(
                client,
                collection_name,
                filter="id >= 1",
                output_fields=["id", *self.feature_names],
                limit=self.total_nb,
            )
            self._assert_rows_round_trip(stored_rows, rows)

            result_ids_by_output = {}
            for output, expected_scores in (
                ("raw", expected_raw),
                ("default", expected_default),
            ):
                result_sets = self._search(
                    client,
                    collection_name,
                    resource_name,
                    self.feature_names,
                    output=output,
                    query_vectors=query_vectors,
                )
                self._assert_scores_match(result_sets, expected_scores)
                if output == "default":
                    assert all(0.0 <= hit["distance"] <= 1.0 for hits in result_sets for hit in hits)
                result_ids_by_output[output] = [[hit["id"] for hit in hits] for hits in result_sets]

            assert result_ids_by_output["raw"] == result_ids_by_output["default"]
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(minio_bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_latency_baseline(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: establish comparable ordinary/cold/warm XGBoost latency baselines
        method: alternate 100 ordinary and 100 reranked searches after 20 warmups on the 3x3000 topology
        expected: zero errors, score parity, bounded requests, and reproducible percentile/QPS output
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_latency",
            19600,
        )
        query_vectors = self._generate_query_vectors(19603)

        try:
            started = time.perf_counter()
            cold_result = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
            )
            cold_latency = time.perf_counter() - started
            self._assert_scores_match(cold_result, fixture["expected_scores"])

            for _ in range(20):
                result = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                )
                self._assert_scores_match(result, fixture["expected_scores"])

            ordinary_latencies = []
            xgboost_latencies = []
            for _ in range(100):
                started = time.perf_counter()
                ordinary = self._direct_plain_search(
                    client,
                    fixture["collection_name"],
                    query_vectors,
                )
                ordinary_latencies.append(time.perf_counter() - started)
                assert len(ordinary) == self.nq
                assert all(len(hits) == self.limit for hits in ordinary)

                started = time.perf_counter()
                reranked = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                )
                xgboost_latencies.append(time.perf_counter() - started)
                self._assert_scores_match(reranked, fixture["expected_scores"])

            ordinary_summary = self._latency_summary("ordinary_warm", ordinary_latencies)
            xgboost_summary = self._latency_summary("xgboost_warm", xgboost_latencies)
            assert cold_latency < 30
            assert ordinary_summary["max_ms"] < 30_000
            assert xgboost_summary["max_ms"] < 30_000
            self._log_non_functional_metrics(
                "XGB-NF-PERF-001",
                cold_ms=cold_latency * 1000,
                ordinary=ordinary_summary,
                xgboost=xgboost_summary,
                p95_overhead_ratio=xgboost_summary["p95_ms"] / ordinary_summary["p95_ms"],
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_concurrent_cold_and_warm_search(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: validate concurrent model cold load and warm reuse
        method: synchronize 10 independent clients, then execute 20 searches per client
        expected: all 200 requests succeed and every returned score matches the local model
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_concurrent",
            19610,
        )
        query_vectors = self._generate_query_vectors(19613)
        worker_count = 10
        searches_per_worker = 20
        start_barrier = threading.Barrier(worker_count)

        def run_worker():
            worker_client = self._client()
            first_latency = None
            warm_latencies = []
            try:
                start_barrier.wait(timeout=30)
                for request_idx in range(searches_per_worker):
                    started = time.perf_counter()
                    result = self._direct_xgboost_search(
                        worker_client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                    )
                    latency = time.perf_counter() - started
                    if request_idx == 0:
                        first_latency = latency
                    else:
                        warm_latencies.append(latency)
                    self._assert_scores_match(result, fixture["expected_scores"])
                return first_latency, warm_latencies
            finally:
                self.close(worker_client)

        try:
            wall_started = time.perf_counter()
            first_latencies = []
            warm_latencies = []
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(run_worker) for _ in range(worker_count)]
                for future in as_completed(futures):
                    first_latency, worker_warm_latencies = future.result()
                    first_latencies.append(first_latency)
                    warm_latencies.extend(worker_warm_latencies)
            wall_duration = time.perf_counter() - wall_started

            assert len(first_latencies) == worker_count
            assert len(warm_latencies) == worker_count * (searches_per_worker - 1)
            first_summary = self._latency_summary("concurrent_first_request", first_latencies)
            warm_summary = self._latency_summary("concurrent_warm", warm_latencies)
            assert max(first_summary["max_ms"], warm_summary["max_ms"]) < 30_000
            self._log_non_functional_metrics(
                "XGB-NF-CON-001",
                workers=worker_count,
                requests=worker_count * searches_per_worker,
                wall_seconds=wall_duration,
                wall_qps=(worker_count * searches_per_worker) / wall_duration,
                first_request_latency=first_summary,
                warm_latency=warm_summary,
                errors=0,
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize(
        "sealed_segment_count",
        [2, 4, 8, 10],
        ids=["2-sealed", "4-sealed", "8-sealed", "10-sealed"],
    )
    def test_milvus_client_xgboost_non_functional_segment_scale(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
        sealed_segment_count,
    ):
        """
        target: characterize L0 rerank scaling as segment fan-out grows
        method: search 2/4/8/10 indexed sealed segments plus one growing segment, exactly 3000 rows each
        expected: topology and score parity hold while latency metrics are emitted for every gradient
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            f"xgboost_nf_segments_{sealed_segment_count}",
            19620,
            sealed_segment_count=sealed_segment_count,
            source_row_count=(10 + 1) * self.nb,
        )
        query_vectors = self._generate_query_vectors(19640, nq=10)
        limit = 100

        try:
            started = time.perf_counter()
            cold_result = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
                limit=limit,
                search_param_overrides={"ef": 256},
            )
            cold_latency = time.perf_counter() - started
            self._assert_scores_match(
                cold_result,
                fixture["expected_scores"],
                expected_nq=len(query_vectors),
                expected_limit=limit,
            )

            for _ in range(5):
                warmup = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=limit,
                    search_param_overrides={"ef": 256},
                )
                self._assert_scores_match(
                    warmup,
                    fixture["expected_scores"],
                    expected_nq=len(query_vectors),
                    expected_limit=limit,
                )

            latencies = []
            for _ in range(20):
                started = time.perf_counter()
                result = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=limit,
                    search_param_overrides={"ef": 256},
                )
                latencies.append(time.perf_counter() - started)
                self._assert_scores_match(
                    result,
                    fixture["expected_scores"],
                    expected_nq=len(query_vectors),
                    expected_limit=limit,
                )

            summary = self._latency_summary(f"segments_{sealed_segment_count}", latencies)
            assert summary["max_ms"] < 30_000
            self._log_non_functional_metrics(
                "XGB-NF-SCALE-001",
                sealed_segments=sealed_segment_count,
                growing_segments=1,
                rows_per_segment=self.nb,
                total_rows=fixture["row_count"],
                nq=len(query_vectors),
                topk=limit,
                model_trees=40,
                cold_ms=cold_latency * 1000,
                latency=summary,
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_query_shape_scale(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: characterize XGBoost search as nq/topK grow independently of collection/model setup
        method: run 1/10, 10/100, and 50/500 over one 3x3000 HNSW collection and one 40-tree model
        expected: every shape and score remains correct while per-gradient latency is recorded
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_query_shape",
            19645,
        )
        query_pool = self._generate_query_vectors(19648, nq=50)
        scenarios = [(1, 10), (10, 100), (50, 500)]

        try:
            summaries = []
            for nq, topk in scenarios:
                query_vectors = query_pool[:nq]
                search_params = {"ef": max(256, topk)}
                for _ in range(3):
                    warmup = self._direct_xgboost_search(
                        client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                        limit=topk,
                        search_param_overrides=search_params,
                    )
                    self._assert_scores_match(
                        warmup,
                        fixture["expected_scores"],
                        expected_nq=nq,
                        expected_limit=topk,
                    )

                latencies = []
                for _ in range(10):
                    started = time.perf_counter()
                    result = self._direct_xgboost_search(
                        client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                        limit=topk,
                        search_param_overrides=search_params,
                    )
                    latencies.append(time.perf_counter() - started)
                    self._assert_scores_match(
                        result,
                        fixture["expected_scores"],
                        expected_nq=nq,
                        expected_limit=topk,
                    )
                summary = self._latency_summary(f"nq_{nq}_topk_{topk}", latencies)
                assert summary["max_ms"] < 30_000
                summaries.append({"nq": nq, "topk": topk, "latency": summary})

            self._log_non_functional_metrics(
                "XGB-NF-SCALE-002",
                rows=fixture["row_count"],
                sealed_segments=2,
                growing_segments=1,
                model_trees=40,
                scenarios=summaries,
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_model_size_scale(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: characterize XGBoost prediction as tree count and UBJ size grow
        method: run deterministic 40/120/300-tree models on one 3x3000 collection with nq=10/topK=100
        expected: every model matches its local oracle and emits size/latency evidence
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_model_40",
            19655,
            num_boost_round=40,
        )
        query_vectors = self._generate_query_vectors(19658, nq=10)
        resources = [
            {
                "trees": 40,
                "resource_name": fixture["resource_name"],
                "remote_path": fixture["remote_path"],
                "model_path": fixture["model_path"],
                "expected_scores": fixture["expected_scores"],
            }
        ]

        try:
            for trees in (120, 300):
                _, model_path, expected_scores, _ = self._train_xgboost_model(
                    tmp_path,
                    f"xgboost_nf_model_{trees}",
                    fixture["features"],
                    fixture["labels"],
                    "reg:squarederror",
                    num_boost_round=trees,
                )
                resource_name = cf.gen_unique_str(f"xgboost_nf_model_{trees}_resource")
                remote_path = f"xgboost/{resource_name}.ubj"
                model_info = {
                    "trees": trees,
                    "resource_name": resource_name,
                    "remote_path": remote_path,
                    "model_path": model_path,
                    "expected_scores": expected_scores,
                }
                resources.append(model_info)
                self._upload_model(
                    client,
                    minio_host,
                    minio_bucket,
                    resource_name,
                    remote_path,
                    model_path,
                )

            summaries = []
            for model in resources:
                for _ in range(3):
                    warmup = self._direct_xgboost_search(
                        client,
                        fixture["collection_name"],
                        model["resource_name"],
                        query_vectors,
                        limit=100,
                        search_param_overrides={"ef": 256},
                    )
                    self._assert_scores_match(
                        warmup,
                        model["expected_scores"],
                        expected_nq=len(query_vectors),
                        expected_limit=100,
                    )

                latencies = []
                for _ in range(10):
                    started = time.perf_counter()
                    result = self._direct_xgboost_search(
                        client,
                        fixture["collection_name"],
                        model["resource_name"],
                        query_vectors,
                        limit=100,
                        search_param_overrides={"ef": 256},
                    )
                    latencies.append(time.perf_counter() - started)
                    self._assert_scores_match(
                        result,
                        model["expected_scores"],
                        expected_nq=len(query_vectors),
                        expected_limit=100,
                    )
                summary = self._latency_summary(f"trees_{model['trees']}", latencies)
                assert summary["max_ms"] < 30_000
                summaries.append(
                    {
                        "trees": model["trees"],
                        "ubj_bytes": model["model_path"].stat().st_size,
                        "latency": summary,
                    }
                )

            self._log_non_functional_metrics(
                "XGB-NF-SCALE-003",
                rows=fixture["row_count"],
                sealed_segments=2,
                growing_segments=1,
                nq=len(query_vectors),
                topk=100,
                models=summaries,
            )
        finally:
            for model in resources:
                self._cleanup_file_resource(
                    client,
                    fixture["minio_client"],
                    minio_bucket,
                    model["resource_name"],
                    model["remote_path"],
                )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_resource_identity_replacement(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: validate cache lease and stale-model eviction across FileResource identity replacement
        method: search during remove/re-add of the same resource name from model A path to model B path
        expected: successful responses wholly match A or B, the absent window is explicit, and post-sync is stable B
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_cache_a",
            19650,
            num_boost_round=120,
        )
        query_vectors = self._generate_query_vectors(19653, nq=20)
        limit = 100
        _, model_b_path, expected_b, _ = self._train_xgboost_model(
            tmp_path,
            "xgboost_nf_cache_b",
            fixture["features"],
            -fixture["labels"],
            "reg:squarederror",
            num_boost_round=120,
        )
        remote_path_b = f"xgboost/{fixture['resource_name']}_v2.ubj"
        model_b_bytes = model_b_path.read_bytes()
        fixture["minio_client"].put_object(
            minio_bucket,
            remote_path_b,
            io.BytesIO(model_b_bytes),
            len(model_b_bytes),
        )
        expected_a_by_id = {idx + 1: score for idx, score in enumerate(fixture["expected_scores"])}
        expected_b_by_id = {idx + 1: score for idx, score in enumerate(expected_b)}

        def classify(result):
            max_error_a = max(abs(hit["distance"] - expected_a_by_id[hit["id"]]) for hits in result for hit in hits)
            max_error_b = max(abs(hit["distance"] - expected_b_by_id[hit["id"]]) for hits in result for hit in hits)
            if max_error_a <= 1e-5:
                return "A"
            if max_error_b <= 1e-5:
                return "B"
            raise AssertionError(
                f"single response mixed or mismatched model versions: error_a={max_error_a}, error_b={max_error_b}"
            )

        worker_count = 8
        start_barrier = threading.Barrier(worker_count + 1)
        a_done_barrier = threading.Barrier(worker_count + 1)
        absent_start = threading.Event()
        absent_done_barrier = threading.Barrier(worker_count + 1)
        b_start = threading.Event()

        def lifecycle_worker():
            worker_client = self._client()
            try:
                start_barrier.wait(timeout=30)
                result_a = self._direct_xgboost_search(
                    worker_client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=limit,
                    search_param_overrides={"ef": 256},
                )
                assert classify(result_a) == "A"
                a_done_barrier.wait(timeout=30)

                assert absent_start.wait(timeout=30)
                not_found = 0
                try:
                    self._direct_xgboost_search(
                        worker_client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                        limit=limit,
                        search_param_overrides={"ef": 256},
                    )
                except Exception as exc:
                    message = str(exc).lower()
                    if "file resource" in message and "not found" in message:
                        not_found = 1
                    else:
                        raise
                assert not_found == 1
                absent_done_barrier.wait(timeout=30)

                assert b_start.wait(timeout=30)
                phases_b = []
                for _ in range(20):
                    result_b = self._direct_xgboost_search(
                        worker_client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                        limit=limit,
                        search_param_overrides={"ef": 256},
                    )
                    phases_b.append(classify(result_b))
                assert phases_b == ["B"] * len(phases_b)
                return {"a": 1, "not_found": not_found, "b": len(phases_b)}
            finally:
                self.close(worker_client)

        try:
            warm_a = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
                limit=limit,
                search_param_overrides={"ef": 256},
            )
            assert classify(warm_a) == "A"

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(lifecycle_worker) for _ in range(worker_count)]
                start_barrier.wait(timeout=30)
                a_done_barrier.wait(timeout=30)
                self.remove_file_resource(client, fixture["resource_name"])
                assert self._wait_until(
                    lambda: (
                        fixture["resource_name"]
                        not in {resource.name for resource in self.list_file_resources(client)[0]}
                    ),
                    timeout=30,
                    interval=0.2,
                )

                def resource_is_absent_for_search():
                    try:
                        self._direct_xgboost_search(
                            client,
                            fixture["collection_name"],
                            fixture["resource_name"],
                            query_vectors,
                            limit=limit,
                            search_param_overrides={"ef": 256},
                        )
                    except Exception as exc:
                        message = str(exc).lower()
                        if "file resource" in message and "not found" in message:
                            return True
                        raise
                    return False

                assert self._wait_until(resource_is_absent_for_search, timeout=30, interval=0.2)
                absent_start.set()
                absent_done_barrier.wait(timeout=30)

                self.add_file_resource(client, fixture["resource_name"], remote_path_b)
                assert self._wait_until(
                    lambda: (
                        fixture["resource_name"] in {resource.name for resource in self.list_file_resources(client)[0]}
                    ),
                    timeout=30,
                    interval=0.2,
                )
                b_start.set()

                worker_results = []
                for future in as_completed(futures):
                    worker_results.append(future.result())

            assert sum(result["a"] for result in worker_results) == worker_count
            assert sum(result["not_found"] for result in worker_results) == worker_count
            assert sum(result["b"] for result in worker_results) == worker_count * 20

            stable_b = []
            for _ in range(20):
                result = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=limit,
                    search_param_overrides={"ef": 256},
                )
                stable_b.append(classify(result))
            assert stable_b == ["B"] * len(stable_b)
            self._log_non_functional_metrics(
                "XGB-NF-CACHE-001",
                workers=worker_count,
                concurrent_a_responses=worker_count,
                explicit_not_found_during_absent_window=worker_count + 1,
                concurrent_b_responses=worker_count * 20,
                stable_b_responses=len(stable_b),
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )
            try:
                fixture["minio_client"].remove_object(minio_bucket, remote_path_b)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_release_load_cycles(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify repeated collection release/load recovery
        method: perform five release/load cycles on the 3x3000 topology
        expected: row count, hit IDs, order, and local-model score parity remain stable
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_reload",
            19660,
        )
        query_vectors = self._generate_query_vectors(19663)

        try:
            baseline = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
            )
            self._assert_scores_match(baseline, fixture["expected_scores"])
            baseline_ids = [[hit["id"] for hit in hits] for hits in baseline]

            cycle_latencies = []
            for _ in range(5):
                started = time.perf_counter()
                self.release_collection(client, fixture["collection_name"])
                released_state = self.get_load_state(client, fixture["collection_name"])[0]
                assert released_state["state"] == LoadState.NotLoad
                self.load_collection(client, fixture["collection_name"])
                loaded_state = self.get_load_state(client, fixture["collection_name"])[0]
                assert loaded_state["state"] == LoadState.Loaded
                cycle_latencies.append(time.perf_counter() - started)

                count_rows, _ = self.query(
                    client,
                    fixture["collection_name"],
                    filter="id >= 1",
                    output_fields=["count(*)"],
                )
                assert count_rows[0]["count(*)"] == fixture["row_count"]
                result = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                )
                self._assert_scores_match(result, fixture["expected_scores"])
                assert [[hit["id"] for hit in hits] for hits in result] == baseline_ids

            self._log_non_functional_metrics(
                "XGB-NF-REC-001",
                cycles=5,
                rows=fixture["row_count"],
                release_load_latency=self._latency_summary("release_load", cycle_latencies),
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_restart_recovery(
        self,
        request,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify FileResource and XGBoost search recovery after a controlled Milvus deployment restart
        method: record a baseline, rollout restart the explicit standalone or cluster QueryNode deployment, then reconnect
        expected: resource and 9000 rows persist and reranked IDs/scores recover exactly
        """
        kube_context = os.environ.get("MILVUS_KUBE_CONTEXT")
        release = os.environ.get("MILVUS_KUBE_RELEASE")
        if not kube_context or not release:
            pytest.skip("set MILVUS_KUBE_CONTEXT and MILVUS_KUBE_RELEASE for the controlled Kubernetes restart")
        namespace = request.config.getoption("--milvus_ns") or "chaos-testing"
        deployment = os.environ.get("MILVUS_KUBE_DEPLOYMENT") or f"{release}-milvus-standalone"

        def get_target_pod():
            pod_json = subprocess.check_output(
                [
                    "kubectl",
                    "--context",
                    kube_context,
                    "-n",
                    namespace,
                    "get",
                    "pod",
                    "-l",
                    f"app.kubernetes.io/instance={release}",
                    "-o",
                    "json",
                ],
                text=True,
            )
            pod_items = json.loads(pod_json)["items"]
            milvus_pods = [
                pod
                for pod in pod_items
                if pod["metadata"]["name"].startswith(deployment) and not pod["metadata"].get("deletionTimestamp")
            ]
            assert len(milvus_pods) == 1, [pod["metadata"]["name"] for pod in pod_items]
            pod = milvus_pods[0]
            assert any(
                owner.get("kind") == "ReplicaSet" and owner.get("controller")
                for owner in pod["metadata"].get("ownerReferences", [])
            )
            assert any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod["status"].get("conditions", [])
            )
            return pod

        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_restart",
            19670,
        )
        query_vectors = self._generate_query_vectors(19673)
        cleanup_client = client

        try:
            baseline = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
            )
            self._assert_scores_match(baseline, fixture["expected_scores"])
            baseline_ids = [[hit["id"] for hit in hits] for hits in baseline]
            old_pod = get_target_pod()
            old_pod_uid = old_pod["metadata"]["uid"]
            old_image = old_pod["spec"]["containers"][0]["image"]

            restart_started = time.perf_counter()
            subprocess.check_call(
                [
                    "kubectl",
                    "--context",
                    kube_context,
                    "-n",
                    namespace,
                    "rollout",
                    "restart",
                    f"deployment/{deployment}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            subprocess.check_call(
                [
                    "kubectl",
                    "--context",
                    kube_context,
                    "-n",
                    namespace,
                    "rollout",
                    "status",
                    f"deployment/{deployment}",
                    "--timeout=300s",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            restart_seconds = time.perf_counter() - restart_started
            new_pod = get_target_pod()
            assert new_pod["metadata"]["uid"] != old_pod_uid
            assert new_pod["spec"]["containers"][0]["image"] == old_image

            fresh_client = self._client()
            cleanup_client = fresh_client
            assert self._wait_until(
                lambda: (
                    fixture["resource_name"]
                    in {resource.name for resource in self.list_file_resources(fresh_client)[0]}
                ),
                timeout=90,
                interval=2,
            )
            self.load_collection(fresh_client, fixture["collection_name"])
            count_rows, _ = self.query(
                fresh_client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == fixture["row_count"]

            recovered = self._direct_xgboost_search(
                fresh_client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
            )
            self._assert_scores_match(recovered, fixture["expected_scores"])
            assert [[hit["id"] for hit in hits] for hits in recovered] == baseline_ids

            assert all(status.get("restartCount", 0) == 0 for status in new_pod["status"].get("containerStatuses", []))
            self._log_non_functional_metrics(
                "XGB-NF-REC-002",
                deployment=deployment,
                old_pod_uid=old_pod_uid,
                new_pod_uid=new_pod["metadata"]["uid"],
                image=old_image,
                restart_recovery_seconds=restart_seconds,
                rows=fixture["row_count"],
                recovered=True,
            )
        finally:
            self._cleanup_file_resource(
                cleanup_client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_soak(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
        request_duration,
    ):
        """
        target: verify sustained concurrent ordinary and XGBoost search stability
        method: run eight independent clients for the configured duration with alternating control/rerank requests
        expected: zero request errors or score drift and healthy post-soak control/query/rerank paths
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_soak",
            19680,
        )
        query_vectors = self._generate_query_vectors(19683)
        duration_seconds = self._parse_duration_seconds(request_duration)
        worker_count = 8
        ready_barrier = threading.Barrier(worker_count + 1)
        start_event = threading.Event()
        deadline = None

        def soak_worker():
            worker_client = self._client()
            ordinary_latencies = []
            xgboost_latencies = []
            try:
                ready_barrier.wait(timeout=30)
                assert start_event.wait(timeout=30)
                while time.monotonic() < deadline:
                    started = time.perf_counter()
                    ordinary = self._direct_plain_search(
                        worker_client,
                        fixture["collection_name"],
                        query_vectors,
                    )
                    ordinary_latencies.append(time.perf_counter() - started)
                    assert len(ordinary) == self.nq
                    assert all(len(hits) == self.limit for hits in ordinary)

                    started = time.perf_counter()
                    reranked = self._direct_xgboost_search(
                        worker_client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                    )
                    xgboost_latencies.append(time.perf_counter() - started)
                    self._assert_scores_match(reranked, fixture["expected_scores"])
                return ordinary_latencies, xgboost_latencies
            finally:
                self.close(worker_client)

        try:
            ordinary_latencies = []
            xgboost_latencies = []
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(soak_worker) for _ in range(worker_count)]
                ready_barrier.wait(timeout=30)
                deadline = time.monotonic() + duration_seconds
                wall_started = time.perf_counter()
                start_event.set()
                for future in as_completed(futures):
                    worker_ordinary, worker_xgboost = future.result()
                    ordinary_latencies.extend(worker_ordinary)
                    xgboost_latencies.extend(worker_xgboost)
            wall_seconds = time.perf_counter() - wall_started

            assert ordinary_latencies
            assert len(ordinary_latencies) == len(xgboost_latencies)
            ordinary_summary = self._latency_summary("soak_ordinary", ordinary_latencies)
            xgboost_summary = self._latency_summary("soak_xgboost", xgboost_latencies)
            assert max(ordinary_summary["max_ms"], xgboost_summary["max_ms"]) < 30_000
            count_rows, _ = self.query(
                client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == fixture["row_count"]
            post_soak_control = self._direct_plain_search(
                client,
                fixture["collection_name"],
                query_vectors,
            )
            assert len(post_soak_control) == self.nq
            assert all(len(hits) == self.limit for hits in post_soak_control)
            post_soak = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                query_vectors,
            )
            self._assert_scores_match(post_soak, fixture["expected_scores"])
            self._log_non_functional_metrics(
                "XGB-NF-STAB-001",
                configured_duration=request_duration,
                wall_seconds=wall_seconds,
                workers=worker_count,
                ordinary_requests=len(ordinary_latencies),
                xgboost_requests=len(xgboost_latencies),
                wall_qps=(len(ordinary_latencies) + len(xgboost_latencies)) / wall_seconds,
                ordinary_latency=ordinary_summary,
                xgboost_latency=xgboost_summary,
                errors=0,
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_timeout_recovery(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: characterize client deadline behavior after the XGBoost model is warm
        method: warm the model, run an equal-shape ANN control, then issue nq=100/topK=1000 with a 10ms timeout
        expected: timeout is explicit and a normal reranked search succeeds within 30 seconds; native cancel is not claimed
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_timeout",
            19690,
            num_boost_round=300,
        )
        expensive_queries = self._generate_query_vectors(19693, nq=100)
        health_queries = self._generate_query_vectors(19694)

        try:
            warm_started = time.perf_counter()
            warm_result = self._direct_xgboost_search(
                client,
                fixture["collection_name"],
                fixture["resource_name"],
                health_queries,
            )
            model_warm_seconds = time.perf_counter() - warm_started
            self._assert_scores_match(warm_result, fixture["expected_scores"])

            control_started = time.perf_counter()
            plain_control = self._direct_plain_search(
                client,
                fixture["collection_name"],
                expensive_queries,
                limit=1000,
                timeout=30,
                search_param_overrides={"ef": 1024},
            )
            plain_control_seconds = time.perf_counter() - control_started
            assert len(plain_control) == len(expensive_queries)
            assert all(len(hits) == 1000 for hits in plain_control)

            timeout_started = time.perf_counter()
            timeout_error = None
            try:
                self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    expensive_queries,
                    limit=1000,
                    timeout=0.01,
                    search_param_overrides={"ef": 1024},
                )
            except Exception as exc:
                timeout_error = exc
            timeout_seconds = time.perf_counter() - timeout_started
            assert timeout_error is not None, "expensive XGBoost request unexpectedly completed within the 10ms timeout"
            assert any(token in str(timeout_error).lower() for token in ("deadline", "timeout", "timed out")), (
                timeout_error
            )

            recovery_started = time.perf_counter()

            def recovered():
                result = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    health_queries,
                    timeout=5,
                )
                self._assert_scores_match(result, fixture["expected_scores"])
                return True

            assert self._wait_until(recovered, timeout=30, interval=1)
            recovery_seconds = time.perf_counter() - recovery_started
            self._log_non_functional_metrics(
                "XGB-NF-CANCEL-001",
                timeout_seconds=timeout_seconds,
                recovery_seconds=recovery_seconds,
                nq=len(expensive_queries),
                topk=1000,
                trees=300,
                model_warm_seconds=model_warm_seconds,
                plain_control_seconds=plain_control_seconds,
                timeout_error_type=type(timeout_error).__name__,
                assertion_scope="client deadline plus bounded service recovery; native cancellation not proven",
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize(
        "component",
        ["querynode", "proxy", "mixcoord", "datanode", "streamingnode"],
    )
    def test_milvus_client_xgboost_non_functional_chaos_component_pod_kill(
        self,
        request,
        tmp_path,
        minio_host,
        minio_bucket,
        component,
    ):
        """
        target: verify XGBoost correctness and bounded recovery after a cluster component pod kill
        method: search continuously, inject one Chaos Mesh pod-kill, wait for a new Ready pod, then probe recovery
        expected: successful responses never drift and data/resource/search recover within 180 seconds
        """
        kube_context, namespace, release = self._chaos_kube_env(request)
        selector = f"app.kubernetes.io/instance={release},app.kubernetes.io/component={component}"
        old_pod = self._ready_pod(kube_context, namespace, selector)
        assert old_pod is not None, f"expected one Ready {component} pod"
        old_uid = old_pod["metadata"]["uid"]
        chaos_name = f"xgb-fc-{component}-{int(time.time() * 1000) % 100000000}"
        manifest = self._pod_chaos_manifest(
            namespace,
            chaos_name,
            {
                "app.kubernetes.io/instance": release,
                "app.kubernetes.io/component": component,
            },
        )

        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            f"xgboost_nf_chaos_{component}",
            19700 + ["querynode", "proxy", "mixcoord", "datanode", "streamingnode"].index(component) * 10,
        )
        query_vectors = self._generate_query_vectors(19750)
        stop_event = threading.Event()
        worker_ready = threading.Event()
        success_times = []
        request_errors = []
        correctness_errors = []

        def search_worker():
            worker_client = self._client()
            worker_ready.set()
            try:
                while not stop_event.is_set():
                    try:
                        result = self._direct_xgboost_search(
                            worker_client,
                            fixture["collection_name"],
                            fixture["resource_name"],
                            query_vectors,
                            timeout=5,
                        )
                    except Exception as exc:
                        request_errors.append((time.monotonic(), type(exc).__name__, str(exc)[:300]))
                    else:
                        try:
                            self._assert_scores_match(result, fixture["expected_scores"])
                        except AssertionError as exc:
                            correctness_errors.append(str(exc))
                        success_times.append(time.monotonic())
                    stop_event.wait(0.02)
            finally:
                self.close(worker_client)

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(search_worker)
        fresh_client = None
        chaos_applied = False
        injected_at = None
        recovered_at = None
        try:
            assert worker_ready.wait(timeout=30)
            assert self._wait_until(lambda: len(success_times) >= 3, timeout=60, interval=0.2)
            self._apply_chaos(kube_context, namespace, manifest)
            chaos_applied = True
            self._wait_chaos_injected(kube_context, namespace, "podchaos", chaos_name)
            injected_at = time.monotonic()
            new_pod = self._wait_replacement_pod(
                kube_context,
                namespace,
                selector,
                old_uid,
            )
            assert new_pod, f"{component} pod was not replaced within 180 seconds"

            fresh_client = self._client()
            consecutive = 0
            recovery_deadline = time.monotonic() + 180
            while time.monotonic() < recovery_deadline and consecutive < 10:
                try:
                    result = self._direct_xgboost_search(
                        fresh_client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                        timeout=10,
                    )
                    self._assert_scores_match(result, fixture["expected_scores"])
                    consecutive += 1
                    recovered_at = recovered_at or time.monotonic()
                except Exception:
                    consecutive = 0
                    threading.Event().wait(1)
            assert consecutive == 10, f"search did not recover after {component} pod-kill"
            assert not correctness_errors, correctness_errors

            count_rows, _ = self.query(
                fresh_client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == fixture["row_count"]
            assert fixture["resource_name"] in {resource.name for resource in self.list_file_resources(fresh_client)[0]}
            assert self._wait_until(
                lambda: self._all_cluster_components_ready(kube_context, namespace, release),
                timeout=180,
                interval=2,
            )
            self._log_non_functional_metrics(
                "XGB-NF-CHAOS-001",
                component=component,
                chaos_kind="PodChaos",
                chaos_name=chaos_name,
                old_pod_uid=old_uid,
                new_pod_uid=new_pod["metadata"]["uid"],
                successful_requests=len(success_times) + 10,
                transient_request_errors=len(request_errors),
                recovery_seconds=(recovered_at - injected_at) if recovered_at and injected_at else None,
                rows=fixture["row_count"],
            )
        finally:
            stop_event.set()
            try:
                future.result(timeout=30)
            finally:
                executor.shutdown(wait=True)
            if chaos_applied:
                self._delete_chaos(kube_context, namespace, "podchaos", chaos_name)
            self._cleanup_file_resource(
                fresh_client or client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_chaos_cold_load_minio_partition(
        self,
        request,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify FileResource cold sync and XGBoost recovery when MinIO is unreachable
        method: partition QueryNode from MinIO before registering an uploaded model, then remove chaos and retry
        expected: registration sync fails explicitly and observer recovery makes search match the local model
        """
        kube_context, namespace, release = self._chaos_kube_env(request)
        chaos_name = f"xgb-fc-cold-minio-{int(time.time() * 1000) % 100000000}"
        manifest = self._network_partition_manifest(namespace, chaos_name, release)
        client = self._client()
        columns, features, embeddings, _, labels = self._regression_dataset(
            feature_seed=19800,
            embedding_seed=19801,
            query_seed=19802,
        )
        _, model_path, expected_scores, _ = self._train_xgboost_model(
            tmp_path,
            "xgboost_nf_chaos_cold_minio",
            features,
            labels,
            "reg:squarederror",
        )
        resource_name = cf.gen_unique_str("xgboost_nf_chaos_cold_minio_resource")
        remote_path = f"xgboost/{resource_name}.ubj"
        minio_client = self._new_minio_client(minio_host)
        if not minio_client.bucket_exists(minio_bucket):
            minio_client.make_bucket(minio_bucket)
        model_data = model_path.read_bytes()
        minio_client.put_object(
            minio_bucket,
            remote_path,
            io.BytesIO(model_data),
            len(model_data),
        )
        try:
            collection_name = self._create_collection(
                client,
                self._build_rows(columns, embeddings),
                index_type="HNSW",
            )
        except Exception:
            minio_client.remove_object(minio_bucket, remote_path)
            raise
        query_vectors = self._generate_query_vectors(19803)
        chaos_applied = False
        fault_error = None
        recovery_started = None
        try:
            self._apply_chaos(kube_context, namespace, manifest)
            chaos_applied = True
            self._wait_chaos_injected(kube_context, namespace, "networkchaos", chaos_name)
            try:
                client.add_file_resource(resource_name, remote_path, timeout=15)
            except Exception as exc:
                fault_error = exc
            assert fault_error is not None, "FileResource cold sync unexpectedly succeeded during MinIO partition"
            fault_text = str(fault_error).lower()
            assert any(token in fault_text for token in ("sync", "deadline", "timeout", "context", "failed")), (
                fault_error
            )

            self._delete_chaos(kube_context, namespace, "networkchaos", chaos_name)
            chaos_applied = False
            recovery_started = time.monotonic()
            recovered = None
            recovery_deadline = time.monotonic() + 180
            while time.monotonic() < recovery_deadline:
                try:
                    recovered = self._direct_xgboost_search(
                        client,
                        collection_name,
                        resource_name,
                        query_vectors,
                        timeout=15,
                    )
                    self._assert_scores_match(recovered, expected_scores)
                    break
                except Exception:
                    threading.Event().wait(2)
            assert recovered is not None, "FileResource sync did not recover after MinIO partition"
            self._log_non_functional_metrics(
                "XGB-NF-CHAOS-002",
                chaos_kind="NetworkChaos",
                chaos_name=chaos_name,
                fault_error_type=type(fault_error).__name__,
                recovery_seconds=time.monotonic() - recovery_started,
                rows=self.total_nb,
                recovered=True,
            )
        finally:
            if chaos_applied:
                self._delete_chaos(kube_context, namespace, "networkchaos", chaos_name)
            self._cleanup_file_resource(
                client,
                minio_client,
                minio_bucket,
                resource_name,
                remote_path,
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_chaos_warm_cache_minio_pod_kill(
        self,
        request,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify warm QueryNode XGBoost cache correctness across a MinIO pod kill
        method: warm the model, kill the dedicated MinIO pod once, then wait for storage and search recovery
        expected: every successful response keeps score parity and FileResource/data remain usable
        """
        kube_context, namespace, release = self._chaos_kube_env(request)
        selector = f"app=minio,release={release}-minio"
        old_pod = self._ready_pod(kube_context, namespace, selector)
        assert old_pod is not None, "expected one Ready MinIO pod"
        old_uid = old_pod["metadata"]["uid"]
        chaos_name = f"xgb-fc-warm-minio-{int(time.time() * 1000) % 100000000}"
        manifest = self._pod_chaos_manifest(
            namespace,
            chaos_name,
            {"app": "minio", "release": f"{release}-minio"},
        )
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_chaos_warm_minio",
            19810,
        )
        query_vectors = self._generate_query_vectors(19813)
        warm = self._direct_xgboost_search(
            client,
            fixture["collection_name"],
            fixture["resource_name"],
            query_vectors,
        )
        self._assert_scores_match(warm, fixture["expected_scores"])
        chaos_applied = False
        success_count = 0
        request_errors = 0
        try:
            self._apply_chaos(kube_context, namespace, manifest)
            chaos_applied = True
            self._wait_chaos_injected(kube_context, namespace, "podchaos", chaos_name)
            new_pod = self._wait_replacement_pod(
                kube_context,
                namespace,
                selector,
                old_uid,
            )
            assert new_pod, "MinIO pod was not replaced within 180 seconds"
            for _ in range(20):
                try:
                    result = self._direct_xgboost_search(
                        client,
                        fixture["collection_name"],
                        fixture["resource_name"],
                        query_vectors,
                        timeout=10,
                    )
                    self._assert_scores_match(result, fixture["expected_scores"])
                    success_count += 1
                except Exception:
                    request_errors += 1
                    threading.Event().wait(1)
            assert success_count > 0
            assert fixture["minio_client"].bucket_exists(minio_bucket)
            count_rows, _ = self.query(
                client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == fixture["row_count"]
            self._log_non_functional_metrics(
                "XGB-NF-CHAOS-003",
                chaos_kind="PodChaos",
                chaos_name=chaos_name,
                old_pod_uid=old_uid,
                new_pod_uid=new_pod["metadata"]["uid"],
                successful_requests=success_count,
                transient_request_errors=request_errors,
                rows=fixture["row_count"],
            )
        finally:
            if chaos_applied:
                self._delete_chaos(kube_context, namespace, "podchaos", chaos_name)
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_multi_replica_segment_fanout(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify replica-2 loading and segment fan-out for XGBoost L0 rerank
        method: load two 3000-row sealed segments on two replicas, retain a 3000-row growing batch, and search
        expected: every sealed segment has two QueryNode node IDs and all data/scores remain correct
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_ha_fanout",
            19900,
            replica_number=2,
        )
        query_vectors = self._generate_query_vectors(19903, nq=20)

        try:
            topology = self._assert_multi_replica_topology(
                client,
                fixture["collection_name"],
            )
            growing_rows, _ = self.query(
                client,
                fixture["collection_name"],
                filter=f"id > {self.sealed_segment_count * self.nb}",
                output_fields=["id"],
                limit=self.nb,
            )
            assert {row["id"] for row in growing_rows} == set(
                range(self.sealed_segment_count * self.nb + 1, self.total_nb + 1)
            )

            for _ in range(20):
                result = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                )
                self._assert_scores_match(
                    result,
                    fixture["expected_scores"],
                    expected_nq=len(query_vectors),
                )

            count_rows, _ = self.query(
                client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == self.total_nb
            self._log_non_functional_metrics(
                "XGB-NF-HA-001",
                rows=self.total_nb,
                sealed_segments=self.sealed_segment_count,
                growing_rows=self.nb,
                searches=20,
                **topology,
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_multi_replica_cache_identity(
        self,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify FileResource identity invalidates process-local XGBoost caches across two replicas
        method: warm model A, replace the same resource name with model B, then fan concurrent searches across clients
        expected: every post-sync response wholly matches B and no replica returns stale or mixed scores
        """
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_ha_cache_a",
            19910,
            num_boost_round=120,
            replica_number=2,
        )
        query_vectors = self._generate_query_vectors(19913, nq=20)
        limit = 100
        _, model_b_path, expected_b, _ = self._train_xgboost_model(
            tmp_path,
            "xgboost_nf_ha_cache_b",
            fixture["features"],
            -fixture["labels"],
            "reg:squarederror",
            num_boost_round=120,
        )
        remote_path_b = f"xgboost/{fixture['resource_name']}_replica_v2.ubj"
        model_b_bytes = model_b_path.read_bytes()
        fixture["minio_client"].put_object(
            minio_bucket,
            remote_path_b,
            io.BytesIO(model_b_bytes),
            len(model_b_bytes),
        )

        try:
            topology_before = self._assert_multi_replica_topology(
                client,
                fixture["collection_name"],
            )
            for _ in range(20):
                result_a = self._direct_xgboost_search(
                    client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=limit,
                    search_param_overrides={"ef": 256},
                )
                self._assert_scores_match(
                    result_a,
                    fixture["expected_scores"],
                    expected_nq=len(query_vectors),
                    expected_limit=limit,
                )

            self.remove_file_resource(client, fixture["resource_name"])
            assert self._wait_until(
                lambda: (
                    fixture["resource_name"] not in {resource.name for resource in self.list_file_resources(client)[0]}
                ),
                timeout=30,
                interval=0.2,
            )
            self.add_file_resource(client, fixture["resource_name"], remote_path_b)
            assert self._wait_until(
                lambda: fixture["resource_name"] in {resource.name for resource in self.list_file_resources(client)[0]},
                timeout=60,
                interval=0.2,
            )

            worker_count = 8
            searches_per_worker = 20

            def model_b_worker():
                worker_client = self._client()
                try:
                    for _ in range(searches_per_worker):
                        result_b = self._direct_xgboost_search(
                            worker_client,
                            fixture["collection_name"],
                            fixture["resource_name"],
                            query_vectors,
                            limit=limit,
                            search_param_overrides={"ef": 256},
                        )
                        self._assert_scores_match(
                            result_b,
                            expected_b,
                            expected_nq=len(query_vectors),
                            expected_limit=limit,
                        )
                    return searches_per_worker
                finally:
                    self.close(worker_client)

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                responses = [
                    future.result()
                    for future in as_completed([executor.submit(model_b_worker) for _ in range(worker_count)])
                ]
            assert sum(responses) == worker_count * searches_per_worker
            topology_after = self._assert_multi_replica_topology(
                client,
                fixture["collection_name"],
            )
            assert topology_after["replica_nodes"] == topology_before["replica_nodes"]
            self._log_non_functional_metrics(
                "XGB-NF-HA-002",
                model_a_warm_responses=20,
                model_b_responses=sum(responses),
                workers=worker_count,
                identity_switched=True,
                topology_before=topology_before,
                topology_after=topology_after,
            )
        finally:
            self._cleanup_file_resource(
                client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )
            try:
                fixture["minio_client"].remove_object(minio_bucket, remote_path_b)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_multi_replica_failover_restart(
        self,
        request,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify XGBoost search HA while one QueryNode is killed and replaced
        method: search continuously on a replica-2 collection, inject one QueryNode pod-kill, and wait for fan-out recovery
        expected: successful responses retain score parity and the replacement restores replica/data/resource state
        """
        kube_context, namespace, release = self._chaos_kube_env(request)
        selector = f"app.kubernetes.io/instance={release},app.kubernetes.io/component=querynode"
        old_pods = self._wait_ready_pods(
            kube_context,
            namespace,
            selector,
            expected_count=2,
        )
        assert old_pods, "expected two Ready QueryNode pods"
        old_uids = {pod["metadata"]["uid"] for pod in old_pods}
        assert len({pod["spec"]["nodeName"] for pod in old_pods}) == 2
        chaos_name = f"xgb-fc-ha-querynode-{int(time.time() * 1000) % 100000000}"
        manifest = self._pod_chaos_manifest(
            namespace,
            chaos_name,
            {
                "app.kubernetes.io/instance": release,
                "app.kubernetes.io/component": "querynode",
            },
        )
        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_ha_failover",
            19920,
            num_boost_round=120,
            replica_number=2,
        )
        query_vectors = self._generate_query_vectors(19923, nq=20)
        stop_event = threading.Event()
        worker_ready = threading.Event()
        successful_requests = []
        request_errors = []
        correctness_errors = []

        def search_worker():
            worker_client = self._client()
            worker_ready.set()
            try:
                while not stop_event.is_set():
                    try:
                        result = self._direct_xgboost_search(
                            worker_client,
                            fixture["collection_name"],
                            fixture["resource_name"],
                            query_vectors,
                            limit=100,
                            timeout=10,
                            search_param_overrides={"ef": 256},
                        )
                    except Exception as exc:
                        request_errors.append((time.monotonic(), type(exc).__name__, str(exc)[:300]))
                    else:
                        try:
                            self._assert_scores_match(
                                result,
                                fixture["expected_scores"],
                                expected_nq=len(query_vectors),
                                expected_limit=100,
                            )
                        except AssertionError as exc:
                            correctness_errors.append(str(exc))
                        successful_requests.append(time.monotonic())
                    stop_event.wait(0.02)
            finally:
                self.close(worker_client)

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(search_worker)
        chaos_applied = False
        injected_at = None
        recovered_at = None
        fresh_client = None
        try:
            assert worker_ready.wait(timeout=30)
            assert self._wait_until(lambda: len(successful_requests) >= 5, timeout=60, interval=0.2)
            topology_before = self._assert_multi_replica_topology(
                client,
                fixture["collection_name"],
            )
            successes_before_fault = len(successful_requests)
            self._apply_chaos(kube_context, namespace, manifest)
            chaos_applied = True
            self._wait_chaos_injected(kube_context, namespace, "podchaos", chaos_name)
            injected_at = time.monotonic()

            def replacement_ready():
                pods = self._ready_pods(kube_context, namespace, selector)
                new_uids = {pod["metadata"]["uid"] for pod in pods}
                if len(pods) != 2 or new_uids == old_uids:
                    return False
                if len(new_uids & old_uids) != 1:
                    return False
                if len({pod["spec"]["nodeName"] for pod in pods}) != 2:
                    return False
                return pods

            new_pods = self._wait_until(replacement_ready, timeout=180, interval=2)
            assert new_pods, "one QueryNode was not replaced and rescheduled within 180 seconds"
            new_uids = {pod["metadata"]["uid"] for pod in new_pods}
            killed_uid = next(iter(old_uids - new_uids))
            replacement_uid = next(iter(new_uids - old_uids))

            fresh_client = self._client()
            topology_after = self._assert_multi_replica_topology(
                fresh_client,
                fixture["collection_name"],
                timeout=180,
            )
            consecutive = 0
            while consecutive < 10:
                result = self._direct_xgboost_search(
                    fresh_client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=100,
                    timeout=10,
                    search_param_overrides={"ef": 256},
                )
                self._assert_scores_match(
                    result,
                    fixture["expected_scores"],
                    expected_nq=len(query_vectors),
                    expected_limit=100,
                )
                consecutive += 1
                recovered_at = recovered_at or time.monotonic()

            assert not correctness_errors, correctness_errors
            assert len(successful_requests) > successes_before_fault
            assert len(request_errors) < max(20, len(successful_requests))
            count_rows, _ = self.query(
                fresh_client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == self.total_nb
            assert fixture["resource_name"] in {resource.name for resource in self.list_file_resources(fresh_client)[0]}
            assert self._wait_until(
                lambda: self._cluster_component_counts_ready(
                    kube_context,
                    namespace,
                    release,
                    {
                        "mixcoord": 1,
                        "proxy": 2,
                        "querynode": 2,
                        "datanode": 1,
                        "streamingnode": 2,
                    },
                ),
                timeout=180,
                interval=2,
            )
            self._log_non_functional_metrics(
                "XGB-NF-HA-003",
                chaos_kind="PodChaos",
                chaos_name=chaos_name,
                old_pod_uids=sorted(old_uids),
                new_pod_uids=sorted(new_uids),
                killed_pod_uid=killed_uid,
                replacement_pod_uid=replacement_uid,
                successful_requests=len(successful_requests) + consecutive,
                transient_request_errors=len(request_errors),
                correctness_errors=len(correctness_errors),
                recovery_seconds=(recovered_at - injected_at) if recovered_at and injected_at else None,
                rows=self.total_nb,
                topology_before=topology_before,
                topology_after=topology_after,
            )
        finally:
            stop_event.set()
            try:
                future.result(timeout=30)
            finally:
                executor.shutdown(wait=True)
            if chaos_applied:
                self._delete_chaos(kube_context, namespace, "podchaos", chaos_name)
            self._cleanup_file_resource(
                fresh_client or client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_client_xgboost_non_functional_multi_replica_rolling_image_update(
        self,
        request,
        tmp_path,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify the Operator rolling image-update path preserves replica-2 XGBoost availability
        method: search continuously while changing the CR image from its tag to the same build digest
        expected: both QueryNode UIDs change, no request drifts/fails, and replica/data/resource state recovers
        """
        kube_context, namespace, release = self._chaos_kube_env(request)
        target_image = os.environ.get("MILVUS_ROLLING_IMAGE")
        assert target_image, "set MILVUS_ROLLING_IMAGE to the immutable image used for rolling validation"
        selector = f"app.kubernetes.io/instance={release},app.kubernetes.io/component=querynode"
        old_pods = self._wait_ready_pods(
            kube_context,
            namespace,
            selector,
            expected_count=2,
        )
        assert old_pods, "expected two Ready QueryNode pods before rolling update"
        old_uids = {pod["metadata"]["uid"] for pod in old_pods}
        old_images = {pod["spec"]["containers"][0]["image"] for pod in old_pods}
        assert target_image not in old_images, "rolling target already active; use a distinct immutable reference"

        client = self._client()
        fixture = self._prepare_non_functional_case(
            client,
            tmp_path,
            minio_host,
            minio_bucket,
            "xgboost_nf_ha_rolling",
            19930,
            num_boost_round=120,
            replica_number=2,
        )
        query_vectors = self._generate_query_vectors(19933, nq=20)
        topology_before = self._assert_multi_replica_topology(
            client,
            fixture["collection_name"],
        )
        stop_event = threading.Event()
        worker_ready = threading.Event()
        successful_requests = []
        request_errors = []
        correctness_errors = []

        def search_worker():
            worker_client = self._client()
            worker_ready.set()
            try:
                while not stop_event.is_set():
                    try:
                        result = self._direct_xgboost_search(
                            worker_client,
                            fixture["collection_name"],
                            fixture["resource_name"],
                            query_vectors,
                            limit=100,
                            timeout=10,
                            search_param_overrides={"ef": 256},
                        )
                    except Exception as exc:
                        request_errors.append((time.monotonic(), type(exc).__name__, str(exc)[:300]))
                    else:
                        try:
                            self._assert_scores_match(
                                result,
                                fixture["expected_scores"],
                                expected_nq=len(query_vectors),
                                expected_limit=100,
                            )
                        except AssertionError as exc:
                            correctness_errors.append(str(exc))
                        successful_requests.append(time.monotonic())
                    stop_event.wait(0.02)
            finally:
                self.close(worker_client)

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(search_worker)
        ready_querynode_samples = []
        rolling_started = None
        fresh_client = None
        try:
            assert worker_ready.wait(timeout=30)
            assert self._wait_until(lambda: len(successful_requests) >= 5, timeout=60, interval=0.2)
            rolling_started = time.monotonic()
            subprocess.check_call(
                [
                    "kubectl",
                    "--context",
                    kube_context,
                    "-n",
                    namespace,
                    "patch",
                    "milvus",
                    release,
                    "--type=merge",
                    "-p",
                    json.dumps({"spec": {"components": {"image": target_image}}}),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )

            def rolling_complete():
                pods = self._ready_pods(kube_context, namespace, selector)
                ready_querynode_samples.append(len(pods))
                new_uids = {pod["metadata"]["uid"] for pod in pods}
                images = {pod["spec"]["containers"][0]["image"] for pod in pods}
                milvus = self._kubectl_json(
                    kube_context,
                    namespace,
                    "get",
                    "milvus",
                    release,
                )
                if milvus["spec"]["components"]["image"] != target_image:
                    return False
                if milvus.get("status", {}).get("status") != "Healthy":
                    return False
                if len(pods) != 2 or new_uids & old_uids:
                    return False
                if images != {target_image}:
                    return False
                return pods

            new_pods = self._wait_until(rolling_complete, timeout=600, interval=2)
            if not new_pods:
                ready_pods = self._ready_pods(kube_context, namespace, selector)
                milvus = self._kubectl_json(
                    kube_context,
                    namespace,
                    "get",
                    "milvus",
                    release,
                )
                updated_condition = next(
                    (
                        condition
                        for condition in milvus.get("status", {}).get("conditions", [])
                        if condition.get("type") == "MilvusUpdated"
                    ),
                    {},
                )
                self._log_non_functional_metrics(
                    "XGB-NF-HA-004",
                    outcome="failed",
                    failure_reason="QueryNode rolling image update did not complete within 600 seconds",
                    source_images=sorted(old_images),
                    target_image=target_image,
                    old_pod_uids=sorted(old_uids),
                    ready_querynode_uids=sorted(pod["metadata"]["uid"] for pod in ready_pods),
                    ready_querynode_images=sorted({pod["spec"]["containers"][0]["image"] for pod in ready_pods}),
                    rolling_seconds=time.monotonic() - rolling_started,
                    min_ready_querynodes=min(ready_querynode_samples),
                    successful_requests=len(successful_requests),
                    transient_request_errors=len(request_errors),
                    correctness_errors=len(correctness_errors),
                    cr_status=milvus.get("status", {}).get("status"),
                    cr_current_image=milvus.get("status", {}).get("currentImage"),
                    cr_updated_condition=updated_condition,
                    topology_before=topology_before,
                    upgrade_scope="same build tag-to-digest rolling path; cross-version compatibility not claimed",
                )
            assert new_pods, "QueryNode rolling image update did not complete within 600 seconds"
            new_uids = {pod["metadata"]["uid"] for pod in new_pods}
            assert old_uids.isdisjoint(new_uids)
            assert ready_querynode_samples and min(ready_querynode_samples) >= 1
            assert self._wait_until(
                lambda: self._cluster_component_counts_ready(
                    kube_context,
                    namespace,
                    release,
                    {
                        "mixcoord": 1,
                        "proxy": 2,
                        "querynode": 2,
                        "datanode": 1,
                        "streamingnode": 2,
                    },
                ),
                timeout=300,
                interval=2,
            )

            fresh_client = self._client()
            topology_after = self._assert_multi_replica_topology(
                fresh_client,
                fixture["collection_name"],
                timeout=180,
            )
            for _ in range(10):
                result = self._direct_xgboost_search(
                    fresh_client,
                    fixture["collection_name"],
                    fixture["resource_name"],
                    query_vectors,
                    limit=100,
                    timeout=10,
                    search_param_overrides={"ef": 256},
                )
                self._assert_scores_match(
                    result,
                    fixture["expected_scores"],
                    expected_nq=len(query_vectors),
                    expected_limit=100,
                )

            assert not correctness_errors, correctness_errors
            assert not request_errors, request_errors[:10]
            count_rows, _ = self.query(
                fresh_client,
                fixture["collection_name"],
                filter="id >= 1",
                output_fields=["count(*)"],
            )
            assert count_rows[0]["count(*)"] == self.total_nb
            assert fixture["resource_name"] in {resource.name for resource in self.list_file_resources(fresh_client)[0]}
            self._log_non_functional_metrics(
                "XGB-NF-HA-004",
                source_images=sorted(old_images),
                target_image=target_image,
                old_pod_uids=sorted(old_uids),
                new_pod_uids=sorted(new_uids),
                rolling_seconds=time.monotonic() - rolling_started,
                min_ready_querynodes=min(ready_querynode_samples),
                successful_requests=len(successful_requests) + 10,
                transient_request_errors=len(request_errors),
                correctness_errors=len(correctness_errors),
                rows=self.total_nb,
                topology_before=topology_before,
                topology_after=topology_after,
                upgrade_scope="same build tag-to-digest rolling path; cross-version compatibility not claimed",
            )
        finally:
            stop_event.set()
            try:
                future.result(timeout=30)
            finally:
                executor.shutdown(wait=True)
            self._cleanup_file_resource(
                fresh_client or client,
                fixture["minio_client"],
                minio_bucket,
                fixture["resource_name"],
                fixture["remote_path"],
            )
