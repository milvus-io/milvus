"""Numerical and operator contracts through the public FunctionChain search API."""

import math

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.exceptions import ParamError
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr

STAGES = [FunctionChainStage.L0_RERANK, FunctionChainStage.L1_RERANK, FunctionChainStage.L2_RERANK]
MODES = ["sum", "multiply", "min", "max", "avg", "weighted"]

# Independent, hand-calculated results for (8, 2), (-4, 6), (0, -2), (1.5, .5).
COMBINED = {
    "sum": [10, 2, -2, 2],
    "multiply": [16, -24, 0, 0.75],
    "min": [2, -4, -2, 0.5],
    "max": [8, 6, 0, 1.5],
    "avg": [5, 1, -1, 1],
    "weighted": [15, -11, 1, 2.75],
}
# Rows: (8, 2), (NULL, 6), (-4, NULL), (NULL, NULL).
NULL_COMBINED = {
    "sum": ([10, 6, -4, 0], [10, 6, -4, None]),
    "multiply": ([16, 0, 0, 0], [16, 6, -4, None]),
    "min": ([2, 0, -4, 0], [2, 6, -4, None]),
    "max": ([8, 6, 0, 0], [8, 6, -4, None]),
    "avg": ([5, 3, -2, 0], [5, 6, -4, None]),
    "weighted": ([15, -3, -8, 0], [15, -3, -8, None]),
}


class TestMilvusClientFunctionChainBuiltinIndependent(TestMilvusClientV2Base):
    skip_global_role_cleanup = True

    def _collection(self, client, pairs=None, extra_types=None):
        name = cf.gen_unique_str("fc_builtin")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("a", DataType.DOUBLE, nullable=True)
        schema.add_field("b", DataType.DOUBLE, nullable=True)
        schema.add_field("label", DataType.VARCHAR, max_length=64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
        for field, dtype in (extra_types or {}).items():
            schema.add_field(field, dtype, nullable=True)
        index = self.prepare_index_params(client)[0]
        index.add_index(field_name="vector", index_type="FLAT", metric_type="IP")
        self.create_collection(client, name, schema=schema, index_params=index, consistency_level="Strong")
        pairs = pairs if pairs is not None else [(8, 2), (-4, 6), (0, -2), (1.5, 0.5)]
        rows = [
            {"id": i + 1, "a": a, "b": b, "label": f"row_{i + 1}", "vector": [float(i + 1), 1.0]}
            for i, (a, b) in enumerate(pairs)
        ]
        for row in rows:
            for field in extra_types or {}:
                row[field] = row["a"]
        inserted = self.insert(client, name, rows)[0]
        assert inserted["insert_count"] == len(rows)
        self.flush(client, name)
        self.load_collection(client, name)
        return name, rows

    @staticmethod
    def _combine(mode, policy=None, columns=("a", "b")):
        params = {"mode": mode}
        if mode == "weighted":
            params["weights"] = [2.0, -0.5]
        if policy is not None:
            params["null_policy"] = policy
        return FunctionChainExpr("num_combine", args=tuple(col(c) for c in columns), params=params)

    @staticmethod
    def _finish(chain):
        if chain.stage != FunctionChainStage.L0_RERANK:
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
        return chain

    def _search(self, client, name, chain, size, **kwargs):
        response = self.search(
            client,
            name,
            data=[[1.0, 0.0], [0.0, 1.0]],
            anns_field="vector",
            search_params={"metric_type": "IP"},
            limit=size,
            output_fields=["label"],
            function_chains=chain,
            **kwargs,
        )[0]
        if kwargs.get("check_task") == CheckTasks.err_res:
            # The shared checker only checks the message; enforce the wire code here.
            assert response.code == kwargs["check_items"]["err_code"]
        return response

    @staticmethod
    def _assert_scores(result, rows, scores, ids=None, *, exact=False):
        expected = dict(zip([r["id"] for r in rows], scores))
        order = ids if ids is not None else sorted(expected, key=lambda pk: (-expected[pk], pk))
        expected_scores = [expected[pk] for pk in order]
        if not exact:
            expected_scores = pytest.approx(expected_scores, rel=2e-6, abs=2e-7)
        assert len(result) == 2
        for hits in result:
            assert [h["id"] for h in hits] == order
            assert [h["distance"] for h in hits] == expected_scores
            for hit in hits:
                # Only the requested stored field is projected; temporary/input columns stay private.
                assert hit["entity"] == {"label": f"row_{hit['id']}"}

    def _recover(self, client, name, rows, stage):
        chain = self._finish(FunctionChain(stage).map("$score", fn.num_combine(col("$id"), col("$id"), mode="avg")))
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, [r["id"] for r in rows])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("mode", MODES)
    def test_function_chain_num_combine_modes_match_oracle(self, stage, mode):
        """target: six modes; method: explicit signed inputs; expected: exact IDs and scores."""
        client = self._client()
        name, rows = self._collection(client)
        chain = self._finish(FunctionChain(stage).map("$score", self._combine(mode)))
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, COMBINED[mode])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("policy", ["propagate", "as_zero", "skip"])
    def test_function_chain_num_combine_null_policy_and_final_score(self, stage, mode, policy):
        """target: NULL policies; method: mixed/all NULL; expected: intermediate recovery and boundary rejection."""
        client = self._client()
        name, rows = self._collection(client, [(8, 2), (None, 6), (-4, None), (None, None)])
        values = (
            [COMBINED[mode][0], None, None, None] if policy == "propagate" else NULL_COMBINED[mode][policy == "skip"]
        )
        # A second expression exposes each intermediate NULL as zero without changing non-NULL values.
        chain = FunctionChain(stage).map("combined", self._combine(mode, policy))
        chain.map(
            "$score",
            FunctionChainExpr(
                "num_combine", args=(col("combined"), col("combined")), params={"mode": "avg", "null_policy": "as_zero"}
            ),
        )
        self._assert_scores(
            self._search(client, name, self._finish(chain), len(rows)),
            rows,
            [v if v is not None else 0 for v in values],
        )
        final = self._finish(FunctionChain(stage).map("$score", self._combine(mode, policy)))
        if policy == "as_zero":
            self._assert_scores(self._search(client, name, final, len(rows)), rows, values)
        else:
            self._search(
                client,
                name,
                final,
                len(rows),
                check_task=CheckTasks.err_res,
                check_items={"err_code": 2400, "err_msg": "$score contains null"},
            )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize(
        "params,message",
        [
            ({"mode": "bad"}, "invalid mode"),
            ({"mode": "weighted"}, "requires weights"),
            ({"mode": "weighted", "weights": [1.0]}, "requires 2 weights"),
            ({"mode": "weighted", "weights": [1.0, 2.0, 3.0]}, "requires 2 weights"),
            ({"mode": "weighted", "weights": [1.0, "x"]}, "weights"),
            ({"mode": "weighted", "weights": [True, 1.0]}, "weights"),
            ({"mode": "weighted", "weights": [math.inf, 1.0]}, "must be finite"),
            ({"mode": "weighted", "weights": [math.nan, 1.0]}, "must be finite"),
            ({"mode": "sum", "null_policy": "bad"}, "invalid null_policy"),
            ({"mode": "sum", "null_policy": ""}, "null_policy"),
            ({"mode": "sum", "null_policy": 1}, "null_policy"),
        ],
        ids=[
            "mode",
            "missing_weights",
            "short_weights",
            "long_weights",
            "string_weight",
            "bool_weight",
            "inf_weight",
            "nan_weight",
            "policy",
            "empty_policy",
            "numeric_policy",
        ],
    )
    def test_function_chain_num_combine_server_rejects_invalid_params(self, stage, params, message):
        """target: server validation; method: bypass helper checks; expected: rejection followed by exact recovery."""
        client = self._client()
        name, rows = self._collection(client)
        chain = FunctionChain(stage).map(
            "$score", FunctionChainExpr("num_combine", args=(col("a"), col("b")), params=params)
        )
        self._search(
            client,
            name,
            chain,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": message},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    def test_function_chain_num_combine_float32_boundaries(self, stage):
        """target: finite conversion boundaries; method: cancellation/large/tiny inputs; expected: finite Float32 scores."""
        client = self._client()
        name, rows = self._collection(client, [(16777217.0, 0.0), (1e20, -1e20), (1e-46, 0.0), (-0.0, 0.0)])
        chain = self._finish(FunctionChain(stage).map("$score", self._combine("sum")))
        # These Float32 values are exactly representable; tolerance would hide rounding and underflow errors.
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, [16777216.0, 0.0, 0.0, 0.0], exact=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    def test_function_chain_num_combine_overflow_intermediate_and_boundary(self, stage):
        """target: score overflow; method: overflow temporary versus final; expected: finite recovery or boundary error."""
        client = self._client()
        name, rows = self._collection(client, [(1e30, 1e30), (-1e30, 1e30)])
        chain = FunctionChain(stage).map("overflowed", self._combine("multiply"))
        chain.map("$score", fn.num_combine(col("$id"), col("$id"), mode="avg"))
        self._assert_scores(self._search(client, name, self._finish(chain), len(rows)), rows, [1.0, 2.0])
        invalid = FunctionChain(stage).map("$score", self._combine("multiply"))
        self._search(
            client,
            name,
            invalid,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2400, "err_msg": "$score contains non-finite"},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "params",
        [
            {"mode": "bad"},
            {"mode": "weighted"},
            {"mode": "weighted", "weights": [1.0]},
            {"mode": "weighted", "weights": [True, 1.0]},
            {"mode": "weighted", "weights": ["x", 1.0]},
        ],
        ids=["mode", "missing_weights", "short_weights", "bool_weight", "string_weight"],
    )
    def test_function_chain_num_combine_helper_rejects_invalid_params(self, params):
        """target: SDK helper; method: invalid arguments; expected: local ParamError, distinct from server coverage."""
        with pytest.raises(ParamError):
            fn.num_combine(col("a"), col("b"), **params)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("function", ["gauss", "exp", "linear"])
    @pytest.mark.parametrize("decay", [0.001, 0.5, 0.999], ids=["minimum", "default", "maximum"])
    def test_function_chain_decay_matches_distance_oracle(self, stage, function, decay):
        """target: decay geometry; method: both sides and offset boundaries; expected: independent power/linear oracle."""
        client = self._client()
        # Origin=-2, offset=2, scale=4. Effective scaled distances are 0,0,0,.5,.5,1,1,2,2.
        values = [-2.0, -4.0, 0.0, -6.0, 2.0, -8.0, 4.0, -12.0, 8.0]
        name, rows = self._collection(client, [(v, 0.0) for v in values])
        ratio = [0.0, 0.0, 0.0, 0.5, 0.5, 1.0, 1.0, 2.0, 2.0]
        if function == "gauss":
            scores = [decay ** (r * r) for r in ratio]
        elif function == "exp":
            scores = [decay**r for r in ratio]
        else:
            # Linear decay saturates at decay, including beyond scale.
            scores = [1.0 - (1.0 - decay) * min(r, 1.0) for r in ratio]
        chain = self._finish(
            FunctionChain(stage).map(
                "$score", fn.decay(col("a"), function=function, origin=-2, scale=4, offset=2, decay=decay)
            )
        )
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, scores)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("function", ["gauss", "exp", "linear"])
    def test_function_chain_decay_null_is_preserved_until_score_boundary(self, stage, function):
        """target: nullable decay; method: nullable intermediate then final; expected: NULL propagation and recovery."""
        client = self._client()
        name, rows = self._collection(client, [(None, 0.0), (0.0, 0.0), (2.0, 0.0)])
        expr = fn.decay(col("a"), function=function, origin=0, scale=2, offset=0)
        chain = (
            FunctionChain(stage).map("faded", expr).map("$score", self._combine("avg", "as_zero", ("faded", "faded")))
        )
        self._assert_scores(self._search(client, name, self._finish(chain), len(rows)), rows, [0.0, 1.0, 0.5])
        self._search(
            client,
            name,
            FunctionChain(stage).map("$score", expr),
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2400, "err_msg": "$score contains null"},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize(
        "override,message",
        [
            ({"function": "bad"}, "invalid function"),
            ({"origin": "0"}, "origin"),
            ({"origin": math.nan}, "origin must be finite"),
            ({"scale": 0}, "scale must be > 0"),
            ({"scale": -1}, "scale must be > 0"),
            ({"scale": math.inf}, "scale must be finite"),
            ({"offset": -1}, "offset must be >= 0"),
            ({"decay": 0}, "decay must be 0 < decay < 1"),
            ({"decay": 1}, "decay must be 0 < decay < 1"),
            ({"decay": 0.0009}, "numerical stability"),
            ({"decay": 0.9991}, "numerical stability"),
        ],
        ids=[
            "function",
            "origin_type",
            "origin_nan",
            "zero_scale",
            "negative_scale",
            "infinite_scale",
            "negative_offset",
            "zero_decay",
            "one_decay",
            "below_stable",
            "above_stable",
        ],
    )
    def test_function_chain_decay_server_rejects_invalid_params(self, stage, override, message):
        """target: decay parameter bounds; method: raw expression; expected: server rejection and legal recovery."""
        client = self._client()
        name, rows = self._collection(client)
        params = {"function": "exp", "origin": 0, "scale": 2, "offset": 0, "decay": 0.5, **override}
        chain = FunctionChain(stage).map("$score", FunctionChainExpr("decay", args=(col("a"),), params=params))
        self._search(
            client,
            name,
            chain,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": message},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("decimal", range(7))
    def test_function_chain_round_decimal_signed_values_and_ties(self, stage, decimal):
        """target: Float32 rounding; method: exact binary half values; expected: ties toward positive infinity."""
        client = self._client()
        # Binary fractions avoid decimal input conversion ambiguities at exact ties.
        values = [-2.5, -0.5, 0.0, 0.5, 2.5, 0.125, -0.125, 0.0625, -0.0625]
        expected = {
            0: [-2.0, 0.0, 0.0, 1.0, 3.0, 0.0, 0.0, 0.0, 0.0],
            1: [-2.5, -0.5, 0.0, 0.5, 2.5, 0.1, -0.1, 0.1, -0.1],
            2: [-2.5, -0.5, 0.0, 0.5, 2.5, 0.13, -0.12, 0.06, -0.06],
            3: [-2.5, -0.5, 0.0, 0.5, 2.5, 0.125, -0.125, 0.063, -0.062],
        }.get(decimal, values)
        name, rows = self._collection(client, [(v, 0.0) for v in values], extra_types={"single": DataType.FLOAT})
        chain = self._finish(FunctionChain(stage).map("$score", fn.round_decimal(col("single"), decimal=decimal)))
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, expected)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize(
        "decimal", [-1, 7, 1.5, "2", True], ids=["negative", "too_large", "float", "string", "bool"]
    )
    def test_function_chain_round_decimal_server_rejects_invalid_precision(self, stage, decimal):
        """target: precision validation; method: raw expression bypasses SDK; expected: server rejection and recovery."""
        client = self._client()
        name, rows = self._collection(client)
        chain = FunctionChain(stage).map(
            "$score", FunctionChainExpr("round_decimal", args=(col("$score"),), params={"decimal": decimal})
        )
        self._search(
            client,
            name,
            chain,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "decimal"},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("field", ["a", "id", "label"], ids=["double", "int64", "varchar"])
    def test_function_chain_round_decimal_requires_float32_column(self, stage, field):
        """target: input type; method: non-Float32 stored columns; expected: rejection followed by valid search."""
        client = self._client()
        name, rows = self._collection(client)
        chain = FunctionChain(stage).map("$score", fn.round_decimal(col(field), decimal=2))
        self._search(
            client,
            name,
            chain,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "must be Float32"},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    def test_function_chain_round_decimal_null_intermediate_and_final(self, stage):
        """target: nullable rounding; method: round intermediate NULL then coalesce; expected: NULL survives to boundary."""
        client = self._client()
        name, rows = self._collection(
            client, [(None, 0.0), (0.125, 0.0), (-0.125, 0.0)], extra_types={"single": DataType.FLOAT}
        )
        expr = fn.round_decimal(col("single"), decimal=2)
        chain = (
            FunctionChain(stage)
            .map("rounded", expr)
            .map("$score", self._combine("avg", "as_zero", ("rounded", "rounded")))
        )
        self._assert_scores(self._search(client, name, self._finish(chain), len(rows)), rows, [0.0, 0.13, -0.12])
        self._search(
            client,
            name,
            FunctionChain(stage).map("$score", expr),
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2400, "err_msg": "$score contains null"},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "decimal", [-1, 7, 1.5, "2", True], ids=["negative", "too_large", "float", "string", "bool"]
    )
    def test_function_chain_round_decimal_helper_rejects_invalid_precision(self, decimal):
        """target: SDK precision checks; method: invalid helper arguments; expected: local ParamError."""
        with pytest.raises(ParamError):
            fn.round_decimal(col("$score"), decimal=decimal)

    @staticmethod
    def _group(chain, **overrides):
        from pymilvus.function_chain.chain import FunctionChainOp

        params = {"field": "b", "group_size": 2, "limit": 2, "offset": 0, "scorer": "max", **overrides}
        # RC92 exposes FunctionChainOp but no public generic-op builder; isolate this SDK adapter.
        chain._ops.append(FunctionChainOp("group_by", inputs=(params["field"], "$score", "$id"), params=params))
        return chain

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("scorer,ids", [("max", [1, 2, 4, 5]), ("sum", [4, 5, 1, 2]), ("avg", [4, 5, 6])])
    def test_function_chain_group_by_retains_top_rows_per_group(self, scorer, ids):
        """target: chain grouping; method: repeated groups and max/sum/avg; expected: independent group/row order."""
        client = self._client()
        singleton = 7.25 if scorer == "avg" else 6
        name, rows = self._collection(client, [(10, 1), (4, 1), (1, 1), (8, 2), (7, 2), (singleton, 3)])
        chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
            "$score", fn.num_combine(col("a"), col("a"), mode="avg")
        )
        self._group(chain, scorer=scorer)
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, [10, 4, 1, 8, 7, singleton], ids=ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("position,ids", [("before", [1, 4, 5]), ("after", [1, 2, 4]), ("resort", [1, 4, 5, 2])])
    def test_function_chain_group_by_sort_limit_order(self, position, ids):
        """target: operator order; method: limit before/after grouping or sort after; expected: distinct retained IDs."""
        client = self._client()
        name, rows = self._collection(client, [(10, 1), (4, 1), (1, 1), (8, 2), (7, 2), (6, 3)])
        chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
            "$score", fn.num_combine(col("a"), col("a"), mode="avg")
        )
        chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
        if position == "before":
            chain.limit(3)
        self._group(chain)
        if position == "after":
            chain.limit(3)
        elif position == "resort":
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, [10, 4, 1, 8, 7, 6], ids=ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset,ids", [(0, [1, 2, 3, 4]), (1, [3, 4]), (2, [])])
    def test_function_chain_group_by_null_group_and_offset(self, offset, ids):
        """target: NULL group; method: NULL and repeated finite keys; expected: one NULL group and group offsets."""
        client = self._client()
        name, rows = self._collection(client, [(8, None), (7, None), (6, 1), (5, 1)])
        chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
            "$score", fn.num_combine(col("a"), col("a"), mode="avg")
        )
        self._group(chain, offset=offset)
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, [8, 7, 6, 5], ids=ids)
        empty = self._search(client, name, chain, len(rows), filter="id < 0")
        assert len(empty) == 2 and all(len(hits) == 0 for hits in empty)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "override,message",
        [
            ({"group_size": 0}, "group_size must be positive"),
            ({"group_size": "2"}, "group_size"),
            ({"limit": 0}, "limit must be positive"),
            ({"offset": -1}, "offset must be non-negative"),
            ({"scorer": "bad"}, "invalid group scorer"),
            ({"field": "absent"}, "absent"),
            ({"field": "vector"}, "FloatVector"),
        ],
        ids=["zero_size", "size_type", "zero_limit", "negative_offset", "scorer", "missing_field", "vector_field"],
    )
    def test_function_chain_group_by_rejects_invalid_params(self, override, message):
        """target: grouping validation; method: raw group operator; expected: server error and legal recovery."""
        client = self._client()
        name, rows = self._collection(client)
        chain = self._group(FunctionChain(FunctionChainStage.L2_RERANK), **override)
        self._search(
            client,
            name,
            chain,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2201 if override.get("field") == "absent" else 1100, "err_msg": message},
        )
        self._recover(client, name, rows, FunctionChainStage.L2_RERANK)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES[:2], ids=["l0", "l1"])
    def test_function_chain_group_by_rejects_querynode_stages(self, stage):
        """target: stage limits; method: group operator in L0/L1; expected: rejection and subsequent recovery."""
        client = self._client()
        name, rows = self._collection(client)
        chain = self._group(FunctionChain(stage))
        self._search(
            client,
            name,
            chain,
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": 'type "group_by" is not supported'},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    def test_function_chain_builtin_composition_and_stage_equivalence(self, stage):
        """target: composition; method: decay/weighted/round on full candidate set; expected: same stage-independent oracle."""
        client = self._client()
        name, rows = self._collection(client, [(0, 1), (2, 2), (4, 3), (6, 4)])
        chain = FunctionChain(stage).map("faded", fn.decay(col("a"), function="exp", origin=0, scale=2))
        chain.map("combined", fn.num_combine(col("faded"), col("b"), mode="weighted", weights=[2.0, 1.0]))
        chain.map("$score", fn.round_decimal(col("combined"), decimal=1))
        self._assert_scores(self._search(client, name, self._finish(chain), len(rows)), rows, [3.0, 3.0, 3.5, 4.3])
        if stage != FunctionChainStage.L0_RERANK:
            chain.limit(2, offset=1)
            self._assert_scores(self._search(client, name, chain, len(rows)), rows, [3.0, 3.0, 3.5, 4.3], ids=[3, 1])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize("mode", MODES)
    def test_function_chain_num_combine_default_null_policy(self, stage, mode):
        """target: omitted NULL policy; method: mixed NULL plus finite rows; expected: propagation and legal recovery."""
        client = self._client()
        name, rows = self._collection(client, [(None, 2), (8, 2)])
        chain = FunctionChain(stage).map("combined", self._combine(mode))
        chain.map("$score", self._combine("avg", "as_zero", ("combined", "combined")))
        self._assert_scores(self._search(client, name, self._finish(chain), len(rows)), rows, [0, COMBINED[mode][0]])
        self._search(
            client,
            name,
            FunctionChain(stage).map("$score", self._combine(mode)),
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2400, "err_msg": "$score contains null"},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", STAGES, ids=["l0", "l1", "l2"])
    @pytest.mark.parametrize(
        "columns,message",
        [(("a",), "expected at least 2 input columns"), (("a", "label"), "expected numeric type")],
        ids=["arity", "varchar"],
    )
    def test_function_chain_num_combine_rejects_input_shape(self, stage, columns, message):
        """target: numeric input contract; method: raw expression with arity/type errors; expected: server rejection/recovery."""
        client = self._client()
        name, rows = self._collection(client)
        self._search(
            client,
            name,
            FunctionChain(stage).map("$score", self._combine("sum", columns=columns)),
            len(rows),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": message},
        )
        self._recover(client, name, rows, stage)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "override",
        [{"function": "bad"}, {"origin": "0"}, {"scale": True}, {"offset": []}, {"decay": None}],
        ids=["function", "origin", "scale", "offset", "decay"],
    )
    def test_function_chain_decay_helper_rejects_invalid_types(self, override):
        """target: SDK parameter types; method: invalid helper arguments; expected: local ParamError."""
        params = {"function": "exp", "origin": 0, "scale": 2, **override}
        with pytest.raises(ParamError):
            fn.decay(col("a"), **params)
