# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Struct array nullable predicates regression cases."""

import pytest
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _core_children,
)
from common.struct_array_predicate_helpers import _and, _expression_rows, _not, _or, _positive


class TestMilvusClientStructArrayNullablePredicates(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("quantifier", ["ANY", "ALL", "EXACT"])
    @pytest.mark.parametrize("combination", ["plain", "and", "or", "not"])
    def test_match_predicates_preserve_null_truth_values(self, quantifier, combination):
        """
        target: preserve UNKNOWN inside predicates and on null parent rows
        method: evaluate independent three-valued predicates and outer NOT before/after reload
        expected: exact PK sets, including vacuous ALL and null-parent exclusion under NOT
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, _expression_rows())
        expressions = {
            "plain": "$[score] > 0",
            "and": "$[score] > 0 and $[tags] IS NULL",
            "or": "$[score] > 0 or $[tags] IS NULL",
            "not": "not ($[score] > 0)",
        }

        def predicate(element):
            value = _positive(element)
            if combination == "and":
                return _and(value, element["tags"] is None)
            if combination == "or":
                return _or(value, element["tags"] is None)
            return _not(value) if combination == "not" else value

        expected = {}
        for pk, row in model.items():
            if row["clips"] is None:
                expected[pk] = None
                continue
            matches = [predicate(e) is True for e in row["clips"]]
            expected[pk] = (
                any(matches) if quantifier == "ANY" else all(matches) if quantifier == "ALL" else sum(matches) == 1
            )
        suffix = ", threshold=1" if quantifier == "EXACT" else ""
        expression = f"MATCH_{quantifier}(clips, {expressions[combination]}{suffix})"
        for phase in range(2):
            for negate in (False, True):
                actual = client.query(
                    name,
                    filter=f"not ({expression})" if negate else expression,
                    output_fields=["id"],
                    limit=100,
                    consistency_level="Strong",
                )
                wanted = {pk for pk, value in expected.items() if (_not(value) if negate else value) is True}
                assert {row["id"] for row in actual} == wanted, (phase, expression, negate, actual, wanted)
            if phase == 0:
                self._reload(client, name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "operation", ["contains", "any", "all", "any_empty", "all_empty", "length", "equal", "unequal"]
    )
    def test_nested_row_expressions_and_out_of_bounds(self, operation):
        """
        target: distinguish null/OOB arrays, null leaves and empty arrays
        method: compare row expressions and negation with an independent array-value oracle
        expected: exact PK sets at offsets 0/1/7 in growing and sealed data
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, _expression_rows())
        templates = {
            "contains": "array_contains({path}, 1)",
            "any": "array_contains_any({path}, [1, 3])",
            "all": "array_contains_all({path}, [1, 2])",
            "any_empty": "array_contains_any({path}, [])",
            "all_empty": "array_contains_all({path}, [])",
            "length": "array_length({path}) == 2",
            "equal": "{path} == [1, 2]",
            "unequal": "{path} != [1, 2]",
        }

        def evaluate(tags):
            if tags is None:
                return None
            return {
                "contains": 1 in tags,
                "any": any(v in tags for v in [1, 3]),
                "all": all(v in tags for v in [1, 2]),
                "any_empty": False,
                "all_empty": True,
                "length": len(tags) == 2,
                "equal": tags == [1, 2],
                "unequal": tags != [1, 2],
            }[operation]

        for phase in range(2):
            for offset in (0, 1, 7):
                expression = templates[operation].format(path=f"clips[{offset}][tags]")
                values = {
                    pk: evaluate(row["clips"][offset]["tags"])
                    if row["clips"] is not None and offset < len(row["clips"])
                    else None
                    for pk, row in model.items()
                }
                for negate in (False, True):
                    actual = client.query(
                        name,
                        filter=f"not ({expression})" if negate else expression,
                        output_fields=["id"],
                        limit=100,
                        consistency_level="Strong",
                    )
                    wanted = {pk for pk, value in values.items() if (_not(value) if negate else value) is True}
                    assert {row["id"] for row in actual} == wanted, (phase, expression, negate, actual, wanted)
            if phase == 0:
                self._reload(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expression",
        [
            "clips[score] IS NULL",
            "array_contains(clips[tags], 1)",
            "array_contains_any(clips[tags], [1])",
            "array_contains_all(clips[tags], [1])",
            "clips[tags] == [1]",
            "clips[tags] != [1]",
            "MATCH_ANY(clips, $[tags] > 1)",
            "MATCH_ANY(clips, $[tags] in [1])",
            'MATCH_ANY(clips, $[tags] like "a%")',
            "MATCH_ANY(clips, $[tags][0] == 1)",
        ],
    )
    def test_unsupported_nested_expressions(self, expression):
        """
        target: reject unsupported nested expression syntax without poisoning subsequent queries
        method: execute each expression then a supported element predicate and full row comparison
        expected: server parse/plan rejection and unchanged queryable rows
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, _expression_rows())
        result, ok = self.query(
            client, name, filter=expression, output_fields=["id"], check_task=CheckTasks.check_nothing
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        actual = client.query(name, filter="MATCH_ANY(clips, $[score] IS NULL)", output_fields=["id"], limit=100)
        assert {row["id"] for row in actual} == {
            pk for pk, row in model.items() if any(e["score"] is None for e in row["clips"] or [])
        }
        self._check(client, name, model)
