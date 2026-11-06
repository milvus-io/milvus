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

"""Struct array nullable scalar children regression cases."""

import copy
import json
import random

import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    SCALAR_IDS,
    SCALARS,
    StructArrayNullableBase,
    _child,
    _clips,
    _core_children,
    _record,
    _value,
)
from pymilvus import DataType
from utils.util_log import test_log as log


class TestMilvusClientStructArrayNullableScalarChildren(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    @pytest.mark.parametrize("parent_nullable", [False, True], ids=["required_parent", "nullable_parent"])
    def test_nullable_scalar_child_preserves_null_slots(self, dtype, parent_nullable):
        """
        target: distinguish a null child from a null parent or an empty struct array
        method: all scalar types, independent child masks, omissions, replacement and reload
        expected: every logical element, value and schema nullability matches retained inputs
        """
        client = self._client()
        name = self._create(client, {"anchor": _child(DataType.INT64, False), "value": _child(dtype)}, parent_nullable)
        rows = []
        for n in range(64):
            clips = [{"anchor": j, "value": None if (n + j) % 3 == 0 else _value(dtype, n + j)} for j in range(n % 7)]
            if n % 11 == 0 and parent_nullable:
                clips = None
            rows.append(_record(n, clips))
        model = self._seed(client, name, rows)
        self._check(client, name, model)
        changes = [
            _record(n, [{"anchor": 0, "value": None}, {"anchor": 1, "value": _value(dtype, n)}], revision=1)
            for n in range(0, 72, 3)
        ]
        self.upsert(client, name, changes)
        model.update({r["id"]: r for r in changes})
        omitted = _record(100, [{"anchor": 0}, {"anchor": 1}])
        self.insert(client, name, [omitted])
        model[100] = _record(100, [{"anchor": 0, "value": None}, {"anchor": 1, "value": None}])
        self._check(client, name, model)
        self._reload(client, name)
        self._check(client, name, model)
        actual, _ = self.query(
            client, name, filter="MATCH_ANY(clips, $[value] IS NULL)", output_fields=["id"], limit=1000
        )
        assert {r["id"] for r in actual} == {
            pk for pk, r in model.items() if any(e["value"] is None for e in r["clips"] or [])
        }

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    @pytest.mark.parametrize("seed", [41, 109])
    def test_upsert_delete_reinsert_preserve_nullable_children(self, pk_type, seed):
        """
        target: retain null children and nested arrays through mixed-key mutations
        method: 24 seeded omit/replace/null/empty/delete/reinsert transitions with full models
        expected: acknowledged writes, fields and element predicates survive every reload
        """
        client = self._client()
        name = self._create(client, _core_children(), pk_type=pk_type)
        model = self._seed(client, name, [_record(n, _clips(n), pk_type) for n in range(40)])
        rng = random.Random(seed)
        for step in range(24):
            ids = rng.sample(sorted(model), 2)
            if step % 6 == 4:
                self.delete(client, name, ids=ids)
                for pk in ids:
                    del model[pk]
            else:
                ids.append(_record(1000 + step, [], pk_type)["id"])
                batch = []
                for pk in ids:
                    n = pk if pk_type == "int" else int(pk.rsplit("_", 1)[1])
                    source = _record(n, _clips(n, step + 1), pk_type, step + 1)
                    if step % 6 == 0:
                        patch = {k: source[k] for k in ("id", "normal_vector", "revision")}
                        expected = copy.deepcopy(model.get(pk, {**source, "clips": None}))
                        expected.update(patch)
                    else:
                        source["clips"] = None if step % 6 == 2 else [] if step % 6 == 3 else source["clips"]
                        patch = source
                        expected = copy.deepcopy(source)
                    batch.append(patch)
                    model[pk] = expected
                log.info(f"STRUCT_ENHANCE_HISTORY seed={seed} step={step} payload={json.dumps(batch)}")
                self.upsert(client, name, batch, partial_update=step % 6 != 5)
            self._check(client, name, model)
            if step % 8 == 7:
                self._reload(client, name)
                self._check(client, name, model)
                self._predicates(client, name, model)
