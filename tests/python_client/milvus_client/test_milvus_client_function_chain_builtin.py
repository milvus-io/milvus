"""Numerical and operator contracts through the public FunctionChain search API."""

import math
import struct

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from pymilvus import (
    AnnSearchRequest,
    DataType,
    Function,
    FunctionChain,
    FunctionChainStage,
    FunctionType,
    RRFRanker,
    SearchAggregation,
    TopHits,
    WeightedRanker,
)
from pymilvus.exceptions import ParamError
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr

STAGES = [FunctionChainStage.L0_RERANK, FunctionChainStage.L1_RERANK, FunctionChainStage.L2_RERANK]
MODES = ["sum", "multiply", "min", "max", "avg", "weighted"]

L2_QUERIES = [[1.0, 0.0], [5.0, 0.0]]
L0 = FunctionChainStage.L0_RERANK
L1 = FunctionChainStage.L1_RERANK
L2 = FunctionChainStage.L2_RERANK
L2_SEARCH_CASES = [
    ("none", 1),
    ("aux", 1),
    ("identity_l0", 1),
    ("identity_l1", 1),
    ("l2_reads_distance", 1),
    *[(mode, sign) for mode in ("l0", "l1", "l2", "l0_l1", "all") for sign in (1, -1)],
]

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
        assert len(result) == 2
        for hits in result:
            assert [h["id"] for h in hits] == order
            for hit in hits:
                score = expected[hit["id"]]
                if exact or score == 0:
                    assert hit["distance"] == score
                else:
                    # A fixed absolute floor would accept zero for small nonzero decay scores.
                    assert hit["distance"] == pytest.approx(score, rel=2e-6, abs=0)
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
        """target: Float32 rounding; method: signed ties and fine decimals; expected: distinct precisions and ties toward +inf."""
        client = self._client()
        # Binary fractions avoid decimal input conversion ambiguities at exact ties.
        values = [-2.5, -0.5, 0.0, 0.5, 2.5, 0.125, -0.125, 0.0625, -0.0625]
        expected = {
            0: [-2.0, 0.0, 0.0, 1.0, 3.0, 0.0, 0.0, 0.0, 0.0],
            1: [-2.5, -0.5, 0.0, 0.5, 2.5, 0.1, -0.1, 0.1, -0.1],
            2: [-2.5, -0.5, 0.0, 0.5, 2.5, 0.13, -0.12, 0.06, -0.06],
            3: [-2.5, -0.5, 0.0, 0.5, 2.5, 0.125, -0.125, 0.063, -0.062],
        }.get(decimal, values)
        # Use the stored Float32 input, then quantize the rounded output to Float32.
        # These values distinguish precision 4, 5 and 6, including negative inputs.
        fine_values = [0.1234567, -0.1234567]
        multiplier = 10**decimal
        float_inputs = [struct.unpack("f", struct.pack("f", v))[0] for v in fine_values]
        fine_expected = [math.floor(v * multiplier + 0.5) / multiplier for v in float_inputs]
        values = values + fine_values
        expected = [struct.unpack("f", struct.pack("f", v))[0] for v in expected + fine_expected]
        name, rows = self._collection(client, [(v, 0.0) for v in values], extra_types={"single": DataType.FLOAT})
        chain = self._finish(FunctionChain(stage).map("$score", fn.round_decimal(col("single"), decimal=decimal)))
        self._assert_scores(self._search(client, name, chain, len(rows)), rows, expected, exact=True)

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


class TestMilvusClientFunctionChainL2(TestMilvusClientV2Base):
    """Full candidate recall keeps score expectations independent of worker and segment placement."""

    skip_global_role_cleanup = True

    def _collection(self, client, shards, other_metric="L2"):
        name = cf.gen_unique_str("fc_l2_score")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("ts", DataType.INT64)
        schema.add_field("other_score", DataType.INT64)
        schema.add_field("label", DataType.VARCHAR, max_length=32)
        indexes = self.prepare_index_params(client)[0]
        for field, metric in (("vector", "L2"), ("other_vector", other_metric)):
            schema.add_field(field, DataType.FLOAT_VECTOR, dim=2)
            indexes.add_index(field_name=field, index_type="FLAT", metric_type=metric)
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=shards,
            consistency_level="Strong",
        )
        rows = [
            {
                "id": pk,
                "ts": 10 * pk,
                "other_score": -pk,
                "label": f"row_{pk}",
                "vector": [float(pk), 0.0],
                "other_vector": [float(5 - pk), 0.0],
            }
            for pk in range(1, 5)
        ]
        for batch in (rows[:2], rows[2:]):
            self.insert(client, name, batch)
            self.flush(client, name)
        self.load_collection(client, name)
        return name, rows

    @staticmethod
    def _rewrite(stage, field="ts", sign=1, output="$score"):
        return FunctionChain(stage).map(
            output, fn.num_combine(col(field), col(field), mode="weighted", weights=[sign, sign])
        )

    def _search_chains(self, mode, sign):
        if mode == "none":
            return []
        if mode == "aux":
            return [self._rewrite(L0, output="temporary"), self._rewrite(L1, output="temporary")]
        if mode.startswith("identity_"):
            stage = L0 if mode == "identity_l0" else L1
            return [FunctionChain(stage).map("$score", fn.num_combine(col("$score"), col("$score"), mode="avg"))]
        if mode == "l2_reads_distance":
            return [
                FunctionChain(L2)
                .map("$score", fn.num_combine(col("$score"), col("$score"), mode="sum"))
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            ]
        if mode in ("l0", "l1", "l2"):
            chain = self._rewrite({"l0": L0, "l1": L1, "l2": L2}[mode], sign=sign)
            if mode == "l2":
                chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
            return [chain]
        chains = [
            self._rewrite(L0, sign=sign),
            FunctionChain(L1).map(
                "$score", fn.num_combine(col("$score"), col("ts"), mode="weighted", weights=[1, sign])
            ),
        ]
        if mode == "all":
            chains.append(
                FunctionChain(L2)
                .map("$score", fn.num_combine(col("$score"), col("$score"), mode="sum"))
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
        return list(reversed(chains))

    @staticmethod
    def _assert_hits(results, expected, descending, limit, offset=0):
        assert len(results) == len(L2_QUERIES)
        for hits, scores in zip(results, expected):
            ordered = sorted(scores, key=lambda pk: (-scores[pk] if descending else scores[pk], pk))
            assert [hit["id"] for hit in hits] == ordered[offset : offset + limit]
            for hit in hits:
                assert hit["distance"] == pytest.approx(scores[hit["id"]], rel=2e-6, abs=1e-6)
                assert hit["entity"]["label"] == f"row_{hit['id']}"
                assert "temporary" not in hit["entity"]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards,grouped", [(1, False), (2, False), (1, True), (2, True)])
    @pytest.mark.parametrize("mode,sign", L2_SEARCH_CASES)
    def test_l2_search_score_preservation(self, shards, grouped, mode, sign):
        client = self._client()
        name, rows = self._collection(client, shards)
        chains = self._search_chains(mode, sign)
        kwargs = {"group_by_field": "label", "group_size": 1} if grouped else {}
        expected = []
        for query in L2_QUERIES:
            scores = {}
            for row in rows:
                distance = (row["vector"][0] - query[0]) ** 2
                if mode in ("none", "aux"):
                    score = distance
                elif mode.startswith("identity_"):
                    score = -distance
                elif mode == "l2_reads_distance":
                    score = 2 * distance
                else:
                    factor = {"l0_l1": 3, "all": 6}.get(mode, 2)
                    score = factor * sign * row["ts"]
                scores[row["id"]] = score
            expected.append(scores)
        results = self.search(
            client,
            name,
            data=L2_QUERIES,
            anns_field="vector",
            limit=len(rows),
            search_params={"metric_type": "L2"},
            output_fields=["label"],
            function_chains=chains,
            **kwargs,
        )[0]
        self._assert_hits(results, expected, mode not in ("none", "aux"), len(rows))
        paged_chains = self._search_chains(mode, sign)
        final_chain = next((chain for chain in paged_chains if chain.stage == L2), None)
        if final_chain is None:
            final_chain = FunctionChain(L2)
            paged_chains.append(final_chain)
        final_chain.limit(2, offset=1)
        paged = self.search(
            client,
            name,
            data=L2_QUERIES,
            anns_field="vector",
            limit=len(rows),
            search_params={"metric_type": "L2"},
            output_fields=["label"],
            function_chains=paged_chains,
            **kwargs,
        )[0]
        self._assert_hits(paged, expected, mode not in ("none", "aux"), 2, 1)
        empty = self.search(
            client,
            name,
            data=L2_QUERIES,
            anns_field="vector",
            limit=len(rows),
            filter="id < 0",
            search_params={"metric_type": "L2"},
            output_fields=["label"],
            function_chains=chains,
            **kwargs,
        )[0]
        assert len(empty) == len(L2_QUERIES)
        assert all(not hits for hits in empty)

    @staticmethod
    def _hybrid_oracle(rows, metrics, rewritten, normalize, strategy, empty_first, first_sign=1):
        expected = []
        descending = strategy in ("rrf", "legacy_rrf") or normalize or any(rewritten) or "IP" in metrics
        for query in L2_QUERIES:
            paths = []
            for index, (field, metric) in enumerate(zip(("vector", "other_vector"), metrics)):
                scores = {}
                for row in [] if index == 0 and empty_first else rows:
                    if rewritten[index]:
                        value = 2 * first_sign * row["ts"] if index == 0 else 2 * row["other_score"]
                    elif metric == "L2":
                        value = (row[field][0] - query[0]) ** 2
                    else:
                        value = row[field][0] * query[0]
                    scores[row["id"]] = value
                if strategy in ("rrf", "legacy_rrf"):
                    order = sorted(
                        scores, key=lambda pk: (-scores[pk] if rewritten[index] or metric == "IP" else scores[pk], pk)
                    )
                    scores = {pk: 1 / (60 + rank) for rank, pk in enumerate(order, 1)}
                elif rewritten[index] and normalize:
                    scores = {pk: 0.5 + math.atan(value) / math.pi for pk, value in scores.items()}
                elif not rewritten[index]:
                    if metric == "L2" and descending:
                        scores = {pk: 1 - 2 * math.atan(value) / math.pi for pk, value in scores.items()}
                    elif metric == "IP" and normalize:
                        scores = {pk: 0.5 + math.atan(value) / math.pi for pk, value in scores.items()}
                paths.append(scores)
            combined = {}
            for pk in set(paths[0]) | set(paths[1]):
                present = [path[pk] for path in paths if pk in path]
                if strategy in ("weighted", "legacy_weighted"):
                    combined[pk] = 0.25 * paths[0].get(pk, 0) + 0.75 * paths[1].get(pk, 0)
                elif strategy == "max":
                    combined[pk] = max(present)
                elif strategy == "avg":
                    combined[pk] = sum(present) / len(present)
                else:
                    combined[pk] = sum(present)
            expected.append(combined)
        return expected, descending

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards,grouped", [(1, False), (2, False), (1, True), (2, True)])
    @pytest.mark.parametrize("rewrite", ["none", "aux", "l0", "l1", "both", "both_negative"])
    @pytest.mark.parametrize("other_metric", ["L2", "IP"])
    def test_l2_hybrid_merge_score_preservation(self, shards, grouped, rewrite, other_metric):
        client = self._client()
        name, rows = self._collection(client, shards, other_metric)
        first_chains, second_chains = [], []
        first_sign = -1 if rewrite == "both_negative" else 1
        if rewrite == "aux":
            first_chains = [self._rewrite(L0, output="temporary")]
            second_chains = [self._rewrite(L1, "other_score", output="temporary")]
        elif rewrite in ("l0", "l1", "both", "both_negative"):
            first_chains = [self._rewrite(L1 if rewrite == "l1" else L0, sign=first_sign)]
            if rewrite in ("both", "both_negative"):
                second_chains = [self._rewrite(L1, "other_score")]
        rewritten = [rewrite in ("l0", "l1", "both", "both_negative"), rewrite in ("both", "both_negative")]
        metrics = ["L2", other_metric]
        kwargs = {"group_by_field": "label", "group_size": 1} if grouped else {}
        strategies = ["weighted", "sum", "max", "avg", "legacy_weighted"]
        if not grouped:
            strategies += ["rrf", "legacy_rrf"]
        for empty_first in (False, True):
            requests = [
                AnnSearchRequest(
                    data=L2_QUERIES,
                    anns_field=field,
                    param={"metric_type": metric},
                    limit=len(rows),
                    expr="id < 0" if index == 0 and empty_first else "",
                    function_chains=chains,
                )
                for index, (field, metric, chains) in enumerate(
                    zip(("vector", "other_vector"), metrics, (first_chains, second_chains))
                )
            ]
            for strategy in strategies:
                for normalize in [False] if strategy in ("rrf", "legacy_rrf") else [False, True]:
                    expected, descending = self._hybrid_oracle(
                        rows, metrics, rewritten, normalize, strategy, empty_first, first_sign
                    )
                    chains, ranker = None, None
                    if strategy == "legacy_weighted":
                        ranker = WeightedRanker(0.25, 0.75, norm_score=normalize)
                    elif strategy == "legacy_rrf":
                        ranker = RRFRanker(60)
                    else:
                        params = {} if strategy == "rrf" else {"norm_score": normalize}
                        if strategy == "weighted":
                            params["weights"] = [0.25, 0.75]
                        chains = FunctionChain(L2).merge(strategy, **params)
                    results = self.hybrid_search(
                        client,
                        name,
                        requests,
                        ranker,
                        limit=2,
                        offset=1,
                        output_fields=["label"],
                        function_chains=chains,
                        **kwargs,
                    )[0]
                    try:
                        self._assert_hits(results, expected, descending, 2, 1)
                    except AssertionError as error:
                        raise AssertionError(
                            f"strategy={strategy}, norm_score={normalize}, empty_first={empty_first}, "
                            f"rewrite={rewrite}, metrics={metrics}, grouped={grouped}: {error}"
                        ) from error


class TestMilvusClientFunctionChainScoreConsumers(TestMilvusClientV2Base):
    """Exercise consumers that sort/select scores after Proxy reduction."""

    skip_global_role_cleanup = True
    queries = [[0.0, 0.0], [1.0, 0.0]]

    def _collection(self, client, shards, nested=False):
        name = cf.gen_unique_str("fc_score_consumers")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("ts", DataType.INT64)
        schema.add_field("bucket", DataType.VARCHAR, max_length=16)
        schema.add_field("label", DataType.VARCHAR, max_length=16)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
        elements = client.create_struct_field_schema()
        elements.add_field("vec", DataType.FLOAT_VECTOR, dim=2)
        schema.add_field(
            "chunks",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=elements,
            max_capacity=4,
        )
        indexes = self.prepare_index_params(client)[0]
        for field in ("vector", "chunks[vec]"):
            indexes.add_index(field_name=field, index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=shards,
            consistency_level="Strong",
        )
        rows = [
            {
                "id": pk,
                "ts": pk * 10,
                "bucket": "all",
                "label": f"row_{pk}",
                "vector": [position, 0.0],
                "chunks": [{"vec": [x, 0.0]} for x in chunks],
            }
            for pk, position, chunks in [
                (1, 1.0, [1.0, 3.0, 2.0]),
                (2, 2.0, [2.0, 1.5, 1.75]),
                (3, 4.0, [4.0, 0.5, 2.5]),
            ]
        ]
        if nested:
            rows += [dict(row, id=row["id"] + 3, ts=row["ts"] + 30) for row in rows]
            for row in rows:
                row["label"] = "odd" if row["id"] % 2 else "even"
        for batch in (rows[:1], rows[1:]):
            self.insert(client, name, batch)
            self.flush(client, name)
        self.load_collection(client, name)
        return name, rows

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards", [1, 2])
    @pytest.mark.parametrize("rewritten", [False, True], ids=["raw_l2", "l0_score"])
    @pytest.mark.parametrize("direction", [None, "asc", "desc"], ids=["default", "asc", "desc"])
    def test_l2_rewritten_aggregation_top_hits(self, shards, rewritten, direction):
        """L0 scores drive parent top_hits ordering/truncation; explicit sort wins."""
        client = self._client()
        name, rows = self._collection(client, shards)
        chains = [TestMilvusClientFunctionChainL2._rewrite(L0)] if rewritten else []
        sort = [] if direction is None else [{"_score": direction}]
        result = self.search(
            client,
            name,
            data=self.queries,
            anns_field="vector",
            limit=len(rows),
            search_params={"metric_type": "L2"},
            output_fields=["label"],
            function_chains=chains,
            search_aggregation=SearchAggregation(
                fields=["bucket"],
                size=1,
                top_hits=TopHits(size=2, sort=sort),
                # Unique leaf keys preserve all candidates through distributed
                # group reduction, so parent top_hits must really select two.
                sub_aggregation=SearchAggregation(
                    fields=["label"],
                    size=len(rows),
                    top_hits=TopHits(size=1),
                ),
            ),
        )[0]
        assert len(result.agg_buckets) == len(self.queries)
        for query, buckets in zip(self.queries, result.agg_buckets):
            scores = {row["id"]: 2 * row["ts"] if rewritten else (row["vector"][0] - query[0]) ** 2 for row in rows}
            descending = direction == "desc" or (direction is None and rewritten)
            expected = sorted(scores, key=lambda pk: (-scores[pk] if descending else scores[pk], pk))[:2]
            assert len(buckets) == 1
            bucket = buckets[0]
            assert bucket.count == len(rows)
            assert [hit.pk for hit in bucket.hits] == expected
            assert [hit.score for hit in bucket.hits] == pytest.approx([scores[pk] for pk in expected])
            assert [hit.fields["label"] for hit in bucket.hits] == [f"row_{pk}" for pk in expected]
            assert len(bucket.sub_groups) == len(rows)
            leaf_ids = []
            for leaf in bucket.sub_groups:
                assert len(leaf.hits) == 1
                hit = leaf.hits[0]
                leaf_ids.append(hit.pk)
                assert hit.score == pytest.approx(scores[hit.pk])
            assert sorted(leaf_ids) == sorted(scores)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards", [1, 2])
    @pytest.mark.parametrize("stage", [None, L0, L1], ids=["raw_l2", "l0_score", "l1_score"])
    @pytest.mark.parametrize("strategy", ["weighted", "rrf"])
    @pytest.mark.parametrize("collapse_strategy", ["max", "topk_avg"])
    def test_l2_rewritten_element_collapse(self, shards, stage, strategy, collapse_strategy):
        """Mixed element/row Hybrid must collapse by the effective score direction."""
        client = self._client()
        name, rows = self._collection(client, shards)
        # Segcore supplies -distance. Rewriting it to +2*distance deliberately
        # makes the farthest element the best business score, unlike raw L2.
        chains = (
            []
            if stage is None
            else [
                FunctionChain(stage).map(
                    "$score", fn.num_combine(col("$score"), col("$score"), mode="weighted", weights=[-1, -1])
                )
            ]
        )
        element_params = {"metric_type": "L2"}
        if collapse_strategy == "topk_avg":
            element_params["params"] = {"element_scope": {"collapse": {"strategy": "topk_avg", "topk": 2}}}
        requests = [
            AnnSearchRequest(
                data=self.queries,
                anns_field="chunks[vec]",
                param=element_params,
                limit=sum(len(row["chunks"]) for row in rows),
                function_chains=chains,
            ),
            AnnSearchRequest(
                data=self.queries,
                anns_field="vector",
                param={"metric_type": "L2"},
                limit=len(rows),
            ),
        ]
        ranker = WeightedRanker(1.0, 1.0, norm_score=False) if strategy == "weighted" else RRFRanker(60)
        result = self.hybrid_search(client, name, requests, ranker, limit=2, output_fields=["label"])[0]
        assert len(result) == len(self.queries)
        for query, hits in zip(self.queries, result):
            element_scores, row_scores = {}, {}
            for row in rows:
                distances = [(chunk["vec"][0] - query[0]) ** 2 for chunk in row["chunks"]]
                values = distances if stage is None else [2 * distance for distance in distances]
                ordered = sorted(values, reverse=stage is not None)
                # Three elements per row: top-2 must differ from both max and
                # averaging all elements, and must select in the right direction.
                element_scores[row["id"]] = ordered[0] if collapse_strategy == "max" else sum(ordered[:2]) / 2
                row_scores[row["id"]] = (row["vector"][0] - query[0]) ** 2
            if strategy == "rrf":
                element_order = sorted(
                    element_scores, key=lambda pk: (element_scores[pk] if stage is None else -element_scores[pk], pk)
                )
                row_order = sorted(row_scores, key=lambda pk: (row_scores[pk], pk))
                scores = {pk: 1 / (61 + element_order.index(pk)) + 1 / (61 + row_order.index(pk)) for pk in row_scores}
                descending = True
            else:
                scores = {
                    pk: element_scores[pk]
                    + (row_scores[pk] if stage is None else 1 - 2 * math.atan(row_scores[pk]) / math.pi)
                    for pk in row_scores
                }
                descending = stage is not None
            expected = sorted(scores, key=lambda pk: (-scores[pk] if descending else scores[pk], pk))[:2]
            assert [hit["id"] for hit in hits] == expected
            assert len({hit["id"] for hit in hits}) == len(hits)
            assert all("offset" not in hit for hit in hits)
            assert [hit["distance"] for hit in hits] == pytest.approx([scores[pk] for pk in expected], abs=1e-6)
            assert [hit["entity"]["label"] for hit in hits] == [f"row_{pk}" for pk in expected]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards", [1, 2])
    @pytest.mark.parametrize("rewritten", [False, True], ids=["raw_l2", "l0_score"])
    @pytest.mark.parametrize("direction", [None, "asc", "desc"], ids=["default", "asc", "desc"])
    def test_l2_rewritten_nested_top_hits_truncation(self, shards, rewritten, direction):
        """Each child receives three candidates and must choose the correct two."""
        client = self._client()
        name, rows = self._collection(client, shards, nested=True)
        sort = [] if direction is None else [{"_score": direction}]
        chains = [TestMilvusClientFunctionChainL2._rewrite(L0)] if rewritten else []
        result = self.search(
            client,
            name,
            data=self.queries,
            anns_field="vector",
            limit=len(rows),
            search_params={"metric_type": "L2"},
            output_fields=["label"],
            function_chains=chains,
            search_aggregation=SearchAggregation(
                fields=["bucket"],
                size=1,
                # The largest top_hits.size defines groupSize. Keep all three
                # candidates per composite key until child top_hits truncates.
                top_hits=TopHits(size=3, sort=sort),
                sub_aggregation=SearchAggregation(
                    fields=["label"],
                    size=2,
                    top_hits=TopHits(size=2, sort=sort),
                ),
            ),
        )[0]
        assert len(result.agg_buckets) == len(self.queries)
        for query, buckets in zip(self.queries, result.agg_buckets):
            scores = {row["id"]: 2 * row["ts"] if rewritten else (row["vector"][0] - query[0]) ** 2 for row in rows}
            descending = direction == "desc" or (direction is None and rewritten)

            def key(pk):
                return (-scores[pk] if descending else scores[pk], pk)

            assert len(buckets) == 1
            parent = buckets[0]
            assert parent.count == len(rows)
            parent_ids = sorted(scores, key=key)[:3]
            assert [hit.pk for hit in parent.hits] == parent_ids
            assert [hit.score for hit in parent.hits] == pytest.approx([scores[pk] for pk in parent_ids])
            assert len(parent.sub_groups) == 2
            seen = set()
            for child in parent.sub_groups:
                label = next(part["value"] for part in child.key if part["field_name"] == "label")
                seen.add(label)
                candidates = [row["id"] for row in rows if row["label"] == label]
                assert child.count == len(candidates) == 3
                expected = sorted(candidates, key=key)[:2]
                assert [hit.pk for hit in child.hits] == expected
                assert [hit.score for hit in child.hits] == pytest.approx([scores[pk] for pk in expected])
                assert all(hit.fields["label"] == label for hit in child.hits)
            assert seen == {"odd", "even"}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shards", [1, 2])
    @pytest.mark.parametrize("stage", [L0, L1], ids=["l0", "l1"])
    @pytest.mark.parametrize("metric", ["COSINE", "BM25"])
    def test_rewritten_cosine_bm25_normalization(self, shards, stage, metric):
        """Business scores, including negative/zero values, use IP normalization."""
        client = self._client()
        name = cf.gen_unique_str("fc_rewritten_normalization")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("ts", DataType.DOUBLE)
        schema.add_field("other_score", DataType.DOUBLE)
        indexes = self.prepare_index_params(client)[0]
        if metric == "COSINE":
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
            indexes.add_index(field_name="vector", index_type="FLAT", metric_type=metric)
            field, queries = "vector", [[1.0, 0.0], [0.0, 1.0]]
        else:
            schema.add_field("text", DataType.VARCHAR, max_length=256, enable_analyzer=True)
            schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
            schema.add_function(
                Function(
                    name="text_bm25",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse"],
                )
            )
            indexes.add_index(field_name="sparse", index_type="SPARSE_INVERTED_INDEX", metric_type=metric)
            field, queries = "sparse", ["milvus", "search"]
        self.create_collection(
            client,
            name,
            schema=schema,
            index_params=indexes,
            shards_num=shards,
            consistency_level="Strong",
        )
        rows = [
            {"id": pk, "ts": ts, "other_score": other}
            for pk, ts, other in [(1, -5.0, 1.0), (2, 0.0, -2.0), (3, 5.0, 0.5)]
        ]
        for row in rows:
            if metric == "COSINE":
                row["vector"] = [float(row["id"]), 1.0]
            else:
                # Both queries match every row; original BM25 values are
                # intentionally replaced, so the oracle is corpus-independent.
                row["text"] = "milvus search " * row["id"]
        for batch in (rows[:1], rows[1:]):
            self.insert(client, name, batch)
            self.flush(client, name)
        self.load_collection(client, name)
        requests = [
            AnnSearchRequest(
                data=queries,
                anns_field=field,
                param={"metric_type": metric},
                limit=len(rows),
                function_chains=[TestMilvusClientFunctionChainL2._rewrite(stage, field=score_field)],
            )
            for score_field in ("ts", "other_score")
        ]
        for normalize in [False, True]:

            def transform(value):
                return 0.5 + math.atan(value) / math.pi if normalize else value

            scores = {
                row["id"]: 0.25 * transform(2 * row["ts"]) + 0.75 * transform(2 * row["other_score"]) for row in rows
            }
            expected = sorted(scores, key=lambda pk: (-scores[pk], pk))
            for strategy in ["legacy_weighted", "chain_weighted"]:
                ranker, chains = None, None
                if strategy == "legacy_weighted":
                    ranker = WeightedRanker(0.25, 0.75, norm_score=normalize)
                else:
                    chains = FunctionChain(L2).merge("weighted", weights=[0.25, 0.75], norm_score=normalize)
                result = self.hybrid_search(
                    client,
                    name,
                    requests,
                    ranker,
                    limit=len(rows),
                    function_chains=chains,
                )[0]
                assert len(result) == len(queries)
                for hits in result:
                    context = f"{metric}/{stage}/{strategy}/norm_score={normalize}"
                    assert [hit["id"] for hit in hits] == expected, context
                    assert [hit["distance"] for hit in hits] == pytest.approx(
                        [scores[pk] for pk in expected], abs=1e-6
                    ), context
                    if normalize:
                        assert all(0 <= hit["distance"] <= 1 for hit in hits), context
