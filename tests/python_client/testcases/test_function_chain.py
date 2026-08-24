import io
import time

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, Function, FunctionChain, FunctionChainStage, FunctionScore, FunctionType
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr

prefix = "function_chain"


class TestFunctionChain(TestMilvusClientV2Base):
    """Test pymilvus FunctionChain SDK integration."""

    dim = 2
    vector_field = "vector"
    scalar_field = "ts"

    def _create_function_chain_collection(self, client):
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(self.scalar_field, DataType.INT64)
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)

        return self._create_collection_with_schema_and_rows(
            client,
            collection_name,
            schema,
            [
                {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
                {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
                {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
            ],
        )

    def _create_collection_with_schema_and_rows(self, client, collection_name, schema, rows):
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return collection_name

    def _create_l1_function_chain_collection(self, client):
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(self.scalar_field, DataType.INT64)
        schema.add_field("payload", DataType.INT64)
        schema.add_field("category", DataType.INT64)
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="COSINE")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"collection.autocompaction.enabled": "false"},
        )

        segment_rows = [
            [
                {
                    "id": 1,
                    self.scalar_field: 10,
                    "payload": 101,
                    "category": 1,
                    self.vector_field: [1.0, 0.0],
                },
                {
                    "id": 2,
                    self.scalar_field: 60,
                    "payload": 202,
                    "category": 2,
                    self.vector_field: [0.8, 0.6],
                },
                {
                    "id": 3,
                    self.scalar_field: 30,
                    "payload": 303,
                    "category": 3,
                    self.vector_field: [0.6, 0.8],
                },
            ],
            [
                {
                    "id": 4,
                    self.scalar_field: 50,
                    "payload": 404,
                    "category": 4,
                    self.vector_field: [0.0, 1.0],
                },
                {
                    "id": 5,
                    self.scalar_field: 20,
                    "payload": 505,
                    "category": 5,
                    self.vector_field: [-0.6, 0.8],
                },
                {
                    "id": 6,
                    self.scalar_field: 40,
                    "payload": 606,
                    "category": 6,
                    self.vector_field: [-1.0, 0.0],
                },
            ],
        ]
        for rows in segment_rows:
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        assert self.wait_for_index_ready(client, collection_name, index_name=self.vector_field)
        self.load_collection(client, collection_name)

        deadline = time.time() + 60
        loaded_segments = []
        while time.time() < deadline:
            loaded_segments = client.list_loaded_segments(collection_name)
            if len(loaded_segments) == len(segment_rows):
                break
            time.sleep(1)
        assert len(loaded_segments) == len(segment_rows), loaded_segments
        assert sorted(segment.num_rows for segment in loaded_segments) == [3, 3]
        return collection_name

    def _score_plus_ts_chain(self, stage):
        chain = FunctionChain(stage, name="score_plus_ts").map(
            "$score",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )
        if stage == FunctionChainStage.L2_RERANK:
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
        return chain

    def _assert_search_error(self, client, collection_name, function_chains, err_msg, **kwargs):
        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            function_chains=function_chains,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: err_msg},
            **kwargs,
        )

    @staticmethod
    def _generate_xgboost_model(tmp_path):
        xgb = pytest.importorskip("xgboost")
        features = np.array([[0.1], [0.8], [0.2], [0.9]], dtype=np.float32)
        labels = np.array([0.2, 0.9, 0.4, 0.7], dtype=np.float32)
        dtrain = xgb.DMatrix(features, label=labels)
        booster = xgb.train(
            {
                "objective": "reg:squarederror",
                "max_depth": 2,
                "eta": 1.0,
                "lambda": 0.0,
                "alpha": 0.0,
                "base_score": 0.5,
                "tree_method": "exact",
                "seed": 7,
            },
            dtrain,
            num_boost_round=1,
        )
        model_path = tmp_path / "xgboost_l0_rerank.ubj"
        booster.save_model(model_path)
        expected = booster.predict(dtrain, output_margin=True).astype(float).tolist()
        return model_path, features[:, 0].astype(float).tolist(), expected

    @staticmethod
    def _generate_unsupported_xgboost_model(tmp_path, name, params):
        xgb = pytest.importorskip("xgboost")
        features = np.array([[0.1], [0.8], [0.2], [0.9]], dtype=np.float32)
        labels = np.array([0.0, 1.0, 2.0, 3.0], dtype=np.float32)
        dtrain = xgb.DMatrix(features, label=labels)
        train_params = {
            "objective": "reg:squarederror",
            "max_depth": 2,
            "eta": 1.0,
            "lambda": 0.0,
            "alpha": 0.0,
            "base_score": 0.5,
            "seed": 7,
        }
        train_params.update(params)
        if train_params.get("objective") == "rank:pairwise":
            dtrain.set_group([len(labels)])
        booster = xgb.train(train_params, dtrain, num_boost_round=1)
        model_path = tmp_path / f"{name}.ubj"
        booster.save_model(model_path)
        return model_path

    @staticmethod
    def _new_minio_client(minio_host):
        from minio import Minio

        return Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

    def _upload_file_resource_bytes(self, client, minio_host, bucket, resource_name, remote_path, data):
        minio_client = self._new_minio_client(minio_host)
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
        minio_client.put_object(bucket, remote_path, io.BytesIO(data), len(data))
        self.add_file_resource(client, resource_name, remote_path)
        return minio_client

    def _create_l0_xgboost_collection(self, client, fields, rows):
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        for name, data_type, kwargs in fields:
            schema.add_field(name, data_type, **kwargs)
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
        return self._create_collection_with_schema_and_rows(client, collection_name, schema, rows)

    @staticmethod
    def _l0_xgboost_chain(resource_name, feature_columns, output="raw"):
        return FunctionChain(FunctionChainStage.L0_RERANK, name="l0_xgboost").map(
            "$score",
            FunctionChainExpr(
                "xgboost",
                args=tuple(col(name) for name in feature_columns),
                params={"model_resource": resource_name, "output": output},
            ),
        )

    def _assert_l0_xgboost_search_error(self, client, collection_name, chain, err_msg, limit=3):
        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=limit,
            function_chains=chain,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: err_msg},
        )

    @staticmethod
    def _hit_field(hit, field):
        if field in hit:
            return hit[field]
        return hit.get("entity", {}).get(field)

    @staticmethod
    def _expected_l0_score(hit):
        vector = hit.get("entity", {}).get("vector", hit.get("vector"))
        l2_distance = sum(value * value for value in vector)
        return TestFunctionChain._hit_field(hit, "ts") - l2_distance

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_xgboost_matches_local_predict(self, file_resource_env, tmp_path, minio_host):
        """
        target: test L0 function chain can rerank search results with a real XGBoost UBJ model
        method: generate a tiny XGBoost model locally, upload it as a Milvus file resource, run L0 xgboost rerank
        expected: Milvus search scores and order match local XGBoost raw predictions
        """
        from minio import Minio

        client = self._client()
        resource_name = cf.gen_unique_str("xgboost_model")
        remote_path = f"xgboost/{resource_name}.ubj"
        collection_name = cf.gen_unique_str(prefix)
        model_path, feature_values, expected_scores = self._generate_xgboost_model(tmp_path)

        minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        bucket = file_resource_env["bucket"]
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
        model_bytes = model_path.read_bytes()
        minio_client.put_object(bucket, remote_path, io.BytesIO(model_bytes), len(model_bytes))

        try:
            self.add_file_resource(client, resource_name, remote_path)

            schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("xgb_f0", DataType.FLOAT)
            schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)
            rows = [
                {
                    "id": idx + 1,
                    "xgb_f0": value,
                    self.vector_field: [idx * 0.01, 0.0],
                }
                for idx, value in enumerate(feature_values)
            ]
            self._create_collection_with_schema_and_rows(client, collection_name, schema, rows)

            chain = FunctionChain(FunctionChainStage.L0_RERANK, name="l0_xgboost").map(
                "$score",
                FunctionChainExpr(
                    "xgboost",
                    args=(col("xgb_f0"),),
                    params={"model_resource": resource_name, "output": "raw"},
                ),
            )
            res, _ = self.search(
                client,
                collection_name,
                data=[[0.0, 0.0]],
                anns_field=self.vector_field,
                search_params={"metric_type": "L2"},
                limit=len(rows),
                output_fields=["xgb_f0"],
                function_chains=chain,
            )

            expected_by_id = {idx + 1: score for idx, score in enumerate(expected_scores)}
            expected_ids = [
                idx + 1 for idx, _ in sorted(enumerate(expected_scores), key=lambda item: item[1], reverse=True)
            ]
            assert [hit["id"] for hit in res[0]] == expected_ids
            for hit in res[0]:
                assert abs(hit["distance"]) == pytest.approx(expected_by_id[hit["id"]], rel=1e-5, abs=1e-5)
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_xgboost_missing_resource(self):
        """
        target: test L0 xgboost rejects a model_resource that is not registered
        method: run xgboost rerank with a missing FileResource name
        expected: search fails with file resource not found
        """
        client = self._client()
        rows = [
            {"id": 1, "xgb_f0": 0.1, self.vector_field: [0.0, 0.0]},
            {"id": 2, "xgb_f0": 0.8, self.vector_field: [0.01, 0.0]},
        ]
        collection_name = self._create_l0_xgboost_collection(
            client,
            [("xgb_f0", DataType.FLOAT, {})],
            rows,
        )
        chain = self._l0_xgboost_chain("missing_xgboost_model", ["xgb_f0"])

        self._assert_l0_xgboost_search_error(client, collection_name, chain, "file resource")

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_xgboost_invalid_output_param(self):
        """
        target: test L0 xgboost rejects an invalid output parameter
        method: run xgboost rerank with output=probability
        expected: request fails because output must be default or raw
        """
        client = self._client()
        rows = [
            {"id": 1, "xgb_f0": 0.1, self.vector_field: [0.0, 0.0]},
            {"id": 2, "xgb_f0": 0.8, self.vector_field: [0.01, 0.0]},
        ]
        collection_name = self._create_l0_xgboost_collection(
            client,
            [("xgb_f0", DataType.FLOAT, {})],
            rows,
        )
        chain = self._l0_xgboost_chain("unused_xgboost_model", ["xgb_f0"], output="probability")

        self._assert_l0_xgboost_search_error(client, collection_name, chain, "output must be one of")

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_xgboost_feature_count_mismatch(
        self, file_resource_env, tmp_path, minio_host
    ):
        """
        target: test L0 xgboost rejects feature count mismatches
        method: use a one-feature model with two input feature columns
        expected: search fails with feature column count mismatch
        """
        client = self._client()
        resource_name = cf.gen_unique_str("xgboost_model")
        remote_path = f"xgboost/{resource_name}.ubj"
        model_path, feature_values, _ = self._generate_xgboost_model(tmp_path)
        bucket = file_resource_env["bucket"]
        model_bytes = model_path.read_bytes()
        minio_client = self._upload_file_resource_bytes(
            client, minio_host, bucket, resource_name, remote_path, model_bytes
        )

        try:
            rows = [
                {
                    "id": idx + 1,
                    "xgb_f0": value,
                    "xgb_f1": value + 1.0,
                    self.vector_field: [idx * 0.01, 0.0],
                }
                for idx, value in enumerate(feature_values)
            ]
            collection_name = self._create_l0_xgboost_collection(
                client,
                [("xgb_f0", DataType.FLOAT, {}), ("xgb_f1", DataType.FLOAT, {})],
                rows,
            )
            chain = self._l0_xgboost_chain(resource_name, ["xgb_f0", "xgb_f1"])

            self._assert_l0_xgboost_search_error(
                client, collection_name, chain, "expected 1 feature columns, got 2", limit=len(rows)
            )
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_xgboost_unsupported_input_type(
        self, file_resource_env, tmp_path, minio_host
    ):
        """
        target: test L0 xgboost rejects unsupported input column types
        method: pass a varchar field as an xgboost feature
        expected: search fails with unsupported input column type
        """
        client = self._client()
        resource_name = cf.gen_unique_str("xgboost_model")
        remote_path = f"xgboost/{resource_name}.ubj"
        model_path, feature_values, _ = self._generate_xgboost_model(tmp_path)
        bucket = file_resource_env["bucket"]
        model_bytes = model_path.read_bytes()
        minio_client = self._upload_file_resource_bytes(
            client, minio_host, bucket, resource_name, remote_path, model_bytes
        )

        try:
            rows = [
                {"id": idx + 1, "xgb_text": str(value), self.vector_field: [idx * 0.01, 0.0]}
                for idx, value in enumerate(feature_values)
            ]
            collection_name = self._create_l0_xgboost_collection(
                client,
                [("xgb_text", DataType.VARCHAR, {"max_length": 64})],
                rows,
            )
            chain = self._l0_xgboost_chain(resource_name, ["xgb_text"])

            self._assert_l0_xgboost_search_error(
                client, collection_name, chain, "unsupported input column type", limit=len(rows)
            )
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        "case_name, model_data, expected_error",
        [
            ("not_ubj", b'{"learner":{}}', "failed to parse UBJ model"),
            ("unsupported_objective", None, "unsupported objective"),
            ("unsupported_booster", None, "unsupported booster"),
        ],
    )
    def test_search_rejects_l0_function_chain_xgboost_invalid_model(
        self, file_resource_env, tmp_path, minio_host, case_name, model_data, expected_error
    ):
        """
        target: test L0 xgboost rejects invalid or unsupported model artifacts
        method: register invalid UBJ content, unsupported objective, and unsupported booster models
        expected: search fails while loading the xgboost model
        """
        client = self._client()
        resource_name = cf.gen_unique_str(f"xgboost_{case_name}")
        remote_path = f"xgboost/{resource_name}.ubj"
        if model_data is None:
            if case_name == "unsupported_objective":
                model_path = self._generate_unsupported_xgboost_model(
                    tmp_path, case_name, {"objective": "rank:pairwise"}
                )
            else:
                model_path = self._generate_unsupported_xgboost_model(tmp_path, case_name, {"booster": "gblinear"})
            model_data = model_path.read_bytes()
        bucket = file_resource_env["bucket"]
        minio_client = self._upload_file_resource_bytes(
            client, minio_host, bucket, resource_name, remote_path, model_data
        )

        try:
            rows = [
                {"id": 1, "xgb_f0": 0.1, self.vector_field: [0.0, 0.0]},
                {"id": 2, "xgb_f0": 0.8, self.vector_field: [0.01, 0.0]},
            ]
            collection_name = self._create_l0_xgboost_collection(
                client,
                [("xgb_f0", DataType.FLOAT, {})],
                rows,
            )
            chain = self._l0_xgboost_chain(resource_name, ["xgb_f0"])

            self._assert_l0_xgboost_search_error(client, collection_name, chain, expected_error, limit=len(rows))
        finally:
            try:
                client.remove_file_resource(name=resource_name)
            except Exception:
                pass
            try:
                minio_client.remove_object(bucket, remote_path)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_sdk_reranks_by_scalar_field(self):
        """
        target: test pymilvus FunctionChain SDK with L0 rerank
        method: map $score = num_combine($score, ts) at L0 stage
        expected: search succeeds and result order follows rewritten score
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field, self.vector_field],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]
        expected_scores = [self._expected_l0_score(hit) for hit in res[0]]
        assert expected_scores == sorted(expected_scores, reverse=True)
        assert [pytest.approx(abs(hit["distance"]), rel=1e-5) for hit in res[0]] == expected_scores

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_sdk_uses_hidden_input_field(self):
        """
        target: test L0 FunctionChain SDK can use fields that are not returned
        method: rerank by ts while only requesting primary key output
        expected: search succeeds, result order follows ts, and ts is not returned
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=["id"],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert all(self._hit_field(hit, self.scalar_field) is None for hit in res[0])

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_sdk_can_read_id_system_input(self):
        """
        target: test L0 FunctionChain SDK can read public system input $id
        method: map $score = num_combine($score, $id) at L0 stage
        expected: search succeeds and result order follows rewritten score
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="score_plus_id").map(
            "$score",
            fn.num_combine(col("$score"), col("$id"), mode="sum"),
        )

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            function_chains=chain,
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_sort_op(self):
        """
        target: test L0 FunctionChain SDK rejects non-map operators
        method: use sort op at L0 stage
        expected: request fails because public L0 currently only supports map op
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_sort").sort(
            col("$score"),
            desc=True,
            tie_break_col=col("$id"),
        )

        self._assert_search_error(
            client, collection_name, chain, 'type "sort" is not supported by L0 rerank function chain'
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_write_readonly_system_column(self):
        """
        target: test L0 FunctionChain SDK rejects writes to read-only system columns
        method: write map output to $id
        expected: request fails because only $score is writable in public L0 chains
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_write_id").map(
            "$id",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$id" is not writable')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_read_internal_system_input(self):
        """
        target: test L0 FunctionChain SDK rejects internal system input columns
        method: read $seg_offset from a map expression
        expected: request fails because public L0 only exposes $id and $score as readable system inputs
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_seg_offset_input").map(
            "$score",
            fn.num_combine(col("$seg_offset"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$seg_offset" is not readable')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_read_unknown_system_input(self):
        """
        target: test L0 FunctionChain SDK rejects unknown system input columns
        method: read $tmp_score from a map expression before it is produced
        expected: request fails because users cannot invent new $-prefixed system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_unknown_system_input").map(
            "$score",
            fn.num_combine(col("$tmp_score"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$tmp_score" is not readable')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_reserved_temp_output(self):
        """
        target: test L0 FunctionChain SDK rejects user temporary columns in system namespace
        method: write a map output named $tmp_score
        expected: request fails because $ prefix is reserved for system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_reserved_temp_output").map(
            "$tmp_score",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$tmp_score" is not writable')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_with_function_score(self):
        """
        target: test search rejects ambiguous L0 rerank APIs
        method: send boost FunctionScore and L0 function chain together
        expected: request fails because function_score and function_chains are mutually exclusive
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        function = Function(
            name="boost_ts",
            function_type=FunctionType.RERANK,
            input_field_names=[],
            output_field_names=[],
            params={"reranker": "boost", "weight": "1.5"},
        )
        function_score = FunctionScore(functions=[function])

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
            "function_chains and ranker cannot be used together",
            ranker=function_score,
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_with_order_by(self):
        """
        target: test search rejects order_by with L0 function rerank
        method: send order_by_fields and L0 function chain together
        expected: request fails because they define conflicting sort criteria
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
            "order_by and function rerank cannot be used together",
            order_by_fields=[{"field": self.scalar_field, "order": "asc"}],
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l1_function_chain_multi_query_temp_columns_offset_and_output_alignment(self):
        """
        target: test complex L1 rerank independently processes query chunks across sealed segments
        method: build a temporary score, rewrite score with two hidden fields, sort, offset, and limit two queries
        expected: each query selects exact cross-segment rows and keeps payload aligned without exposing inputs
        """
        client = self._client()
        collection_name = self._create_l1_function_chain_collection(client)
        chain = (
            FunctionChain(FunctionChainStage.L1_RERANK, name="l1_complex")
            .map(
                "boosted_score",
                fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
            )
            .map(
                "$score",
                fn.num_combine(col("boosted_score"), col("category"), mode="sum"),
            )
            .sort(col(self.scalar_field), desc=True, tie_break_col=col("$id"))
            .limit(2, offset=1)
        )
        query_vectors = [[1.0, 0.0], [0.0, 1.0]]

        res, _ = self.search(
            client,
            collection_name,
            data=query_vectors,
            anns_field=self.vector_field,
            search_params={"metric_type": "COSINE"},
            limit=6,
            output_fields=["payload"],
            function_chains=chain,
        )

        expected_ids = [[4, 6], [4, 6]]
        payload_by_id = {4: 404, 6: 606}
        ts_by_id = {4: 50, 6: 40}
        category_by_id = {4: 4, 6: 6}
        vector_by_id = {4: [0.0, 1.0], 6: [-1.0, 0.0]}
        assert len(res) == len(query_vectors)
        for query_vector, hits, ids in zip(query_vectors, res, expected_ids):
            assert [hit["id"] for hit in hits] == ids
            expected_scores = []
            for entity_id in ids:
                cosine = sum(left * right for left, right in zip(query_vector, vector_by_id[entity_id]))
                expected_scores.append(ts_by_id[entity_id] + category_by_id[entity_id] + cosine)
            assert expected_scores == sorted(expected_scores, reverse=True)

            for hit, expected_score in zip(hits, expected_scores):
                assert self._hit_field(hit, "payload") == payload_by_id[hit["id"]]
                assert abs(hit["distance"]) == pytest.approx(expected_score, rel=1e-5, abs=1e-5)
                assert self._hit_field(hit, self.scalar_field) is None
                assert self._hit_field(hit, "category") is None
                assert self._hit_field(hit, "boosted_score") is None
                assert self._hit_field(hit, "$l1_source_index") is None

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_l1_l2_function_chains_execute_in_stage_order(self):
        """
        target: test L0, L1, and L2 function chains compose in their distributed stage order
        method: add ts at L0, category at L1 with sort/limit, then add payload and sort at L2
        expected: final IDs and scores equal exact L0 to L1 to L2 arithmetic
        """
        client = self._client()
        collection_name = self._create_l1_function_chain_collection(client)
        chains = [
            FunctionChain(FunctionChainStage.L0_RERANK, name="l0_add_ts").map(
                "$score",
                fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
            ),
            (
                FunctionChain(FunctionChainStage.L1_RERANK, name="l1_add_category")
                .map(
                    "$score",
                    fn.num_combine(col("$score"), col("category"), mode="sum"),
                )
                .sort(col(self.scalar_field), desc=True, tie_break_col=col("$id"))
                .limit(3)
            ),
            (
                FunctionChain(FunctionChainStage.L2_RERANK, name="l2_add_payload")
                .map(
                    "$score",
                    fn.num_combine(col("$score"), col("payload"), mode="sum"),
                )
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            ),
        ]

        res, _ = self.search(
            client,
            collection_name,
            data=[[1.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "COSINE"},
            limit=6,
            output_fields=[self.scalar_field, "category", "payload"],
            function_chains=chains,
        )

        assert [hit["id"] for hit in res[0]] == [6, 4, 2]
        expected_by_id = {
            2: 202 + 60 + 2 + 0.8,
            4: 404 + 50 + 4 + 0.0,
            6: 606 + 40 + 6 - 1.0,
        }
        for hit in res[0]:
            entity_id = hit["id"]
            assert hit["distance"] == pytest.approx(
                expected_by_id[entity_id],
                rel=1e-5,
                abs=1e-5,
            )
            assert self._hit_field(hit, "payload") == entity_id * 101
            assert self._hit_field(hit, "category") == entity_id

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        "case_name, chain_factory, expected_error, search_kwargs",
        [
            (
                "write_id",
                lambda: FunctionChain(FunctionChainStage.L1_RERANK, name="bad_l1_write_id").map(
                    "$id",
                    fn.num_combine(col("$score"), col("ts"), mode="sum"),
                ),
                'system output "$id" is not writable by L1',
                {},
            ),
            (
                "read_internal_system_input",
                lambda: FunctionChain(FunctionChainStage.L1_RERANK, name="bad_l1_seg_offset").map(
                    "$score",
                    fn.num_combine(col("$seg_offset"), col("$score"), mode="sum"),
                ),
                'system input "$seg_offset" is not readable by L1',
                {},
            ),
            (
                "write_reserved_provenance_column",
                lambda: FunctionChain(FunctionChainStage.L1_RERANK, name="bad_l1_provenance").map(
                    "$l1_source_index",
                    fn.num_combine(col("$score"), col("ts"), mode="sum"),
                ),
                'system output "$l1_source_index" is not writable by L1',
                {},
            ),
            (
                "order_by_conflict",
                lambda: FunctionChain(FunctionChainStage.L1_RERANK, name="bad_l1_order_by").map(
                    "$score",
                    fn.num_combine(col("$score"), col("ts"), mode="sum"),
                ),
                "order_by and function rerank cannot be used together",
                {"order_by_fields": [{"field": "ts", "order": "asc"}]},
            ),
        ],
        ids=[
            "write-id",
            "read-internal-system-input",
            "write-reserved-provenance-column",
            "order-by-conflict",
        ],
    )
    def test_search_rejects_invalid_l1_function_chain(
        self,
        case_name,
        chain_factory,
        expected_error,
        search_kwargs,
    ):
        """
        target: test L1 server validation rejects invalid outputs, inputs, functions, and search modes
        method: send malformed or incompatible L1 chains through ordinary PyMilvus search
        expected: every request fails with parameter error before any fallback execution
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        self._assert_search_error(
            client,
            collection_name,
            chain_factory(),
            expected_error,
            **search_kwargs,
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_duplicate_l1_function_chain_stage(self):
        """
        target: test ordinary search rejects two L1 chains in one request
        method: send two independently valid L1 score maps
        expected: request fails because each function-chain stage may appear only once
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chains = [
            FunctionChain(FunctionChainStage.L1_RERANK, name="l1_first").map(
                "$score",
                fn.num_combine(col("$score"), col("ts"), mode="sum"),
            ),
            FunctionChain(FunctionChainStage.L1_RERANK, name="l1_second").map(
                "$score",
                fn.num_combine(col("$score"), col("$id"), mode="sum"),
            ),
        ]

        self._assert_search_error(
            client,
            collection_name,
            chains,
            "function chain stage FunctionChainStageL1Rerank appears more than once",
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l2_function_chain_sdk_reranks_by_scalar_field(self):
        """
        target: test pymilvus FunctionChain SDK with L2 rerank
        method: map $score = num_combine($score, ts), then sort by $score desc
        expected: search succeeds and result order follows rewritten score
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l2_function_chain_sdk_uses_hidden_input_field(self):
        """
        target: test L2 FunctionChain SDK can use fields that are not returned
        method: rerank by ts while only requesting primary key output
        expected: search succeeds, result order follows ts, and ts is not returned
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=["id"],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert all(self._hit_field(hit, self.scalar_field) is None for hit in res[0])

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l2_function_chain_sdk_temp_column_not_returned(self):
        """
        target: test L2 FunctionChain SDK can use ordinary temporary columns
        method: write tmp_score, write it back to $score, then sort by $score desc
        expected: search succeeds, rerank order is correct, and tmp_score is not returned
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = (
            FunctionChain(FunctionChainStage.L2_RERANK, name="l2_temp_score")
            .map("tmp_score", fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"))
            .map("$score", fn.num_combine(col("tmp_score"), col("$score"), mode="sum"))
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        )

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
            function_chains=chain,
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]
        assert all(self._hit_field(hit, "tmp_score") is None for hit in res[0])

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l2_function_chain_sdk_limit_op(self):
        """
        target: test L2 FunctionChain SDK supports limit operator
        method: request limit=3 and apply function chain limit op with limit=2
        expected: search succeeds and returns only function-chain-limited results
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="l2_limit").limit(2)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            function_chains=chain,
        )

        assert len(res[0]) == 2

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l2_function_chain_write_readonly_system_column(self):
        """
        target: test L2 FunctionChain SDK rejects writes to read-only system columns
        method: write map output to $id
        expected: request fails because only $score is writable in L2 rerank chains
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_write_id").map(
            "$id",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$id" is not writable')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l2_function_chain_reserved_temp_output(self):
        """
        target: test L2 FunctionChain SDK rejects user temporary columns in system namespace
        method: write a map output named $tmp_score
        expected: request fails because $ prefix is reserved for system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_reserved_temp_output").map(
            "$tmp_score",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$tmp_score" is not writable')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l2_function_chain_read_internal_system_input(self):
        """
        target: test L2 FunctionChain SDK rejects internal system input columns
        method: read $seg_offset from a map expression
        expected: request fails because L2 only exposes selected system inputs
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_seg_offset_input").map(
            "$score",
            fn.num_combine(col("$seg_offset"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$seg_offset" is not supported')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l2_function_chain_read_unknown_system_input(self):
        """
        target: test L2 FunctionChain SDK rejects unknown system input columns
        method: read $tmp_score from a map expression before it is produced
        expected: request fails because users cannot invent new $-prefixed system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_unknown_system_input").map(
            "$score",
            fn.num_combine(col("$tmp_score"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$tmp_score" is not supported')

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l2_function_chain_with_function_score(self):
        """
        target: test search rejects ambiguous L2 rerank APIs
        method: send boost FunctionScore and L2 function chain together
        expected: request fails because function chains and ranker are mutually exclusive
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        function = Function(
            name="boost_ts",
            function_type=FunctionType.RERANK,
            input_field_names=[],
            output_field_names=[],
            params={"reranker": "boost", "weight": "1.5"},
        )
        function_score = FunctionScore(functions=[function])

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
            "function_chains and ranker cannot be used together",
            ranker=function_score,
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l2_function_chain_with_order_by(self):
        """
        target: test search rejects order_by with L2 function rerank
        method: send order_by_fields and L2 function chain together
        expected: request fails because they define conflicting sort criteria
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
            "order_by and function rerank cannot be used together",
            order_by_fields=[{"field": self.scalar_field, "order": "asc"}],
        )
