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

"""Concurrent Struct child removal must preserve inserts of retained children."""

import copy
import threading
from concurrent.futures import ThreadPoolExecutor

import allure
import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import StructArrayNullableBase, _child, _record
from common.struct_array_schema_errors import (
    SUBFIELD_DROP_ISSUE,
    ConcurrentSubfieldDropRejection,
    is_subfield_drop_rejection,
)
from pymilvus import DataType
from pymilvus.exceptions import MilvusException


class TestMilvusClientStructArrayConcurrentSubfieldDrop(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(strict=True, raises=ConcurrentSubfieldDropRejection, reason=SUBFIELD_DROP_ISSUE)
    def test_concurrent_drop_preserves_inserts_that_omit_removed_child(self):
        """
        target: keep inserts valid when they omit a concurrently removed nullable child
        method: warm SDK schema, race four disjoint-key writers against child removal
        expected: all inserts succeed and exact retained fields survive the schema change
        """
        client = self._client()
        name = self._create(client, {"anchor": _child(DataType.INT64, False), "added": _child(DataType.INT64)})
        seed = _record(0, [{"anchor": 0}])
        self.insert(client, name, [seed])
        barrier = threading.Barrier(5)

        def writer(worker):
            rows, errors = [], []
            barrier.wait(timeout=30)
            for step in range(12):
                row = _record(1 + worker * 100 + step, [{"anchor": step}])
                try:
                    result = client.insert(name, [row], timeout=30)
                    assert result["insert_count"] == 1, result
                    rows.append(row)
                except MilvusException as exc:
                    errors.append({"row": row, "code": exc.code, "message": exc.message})
            return rows, errors

        def drop():
            barrier.wait(timeout=30)
            client.drop_collection_field(name, field_name="clips[added]", timeout=60)

        with ThreadPoolExecutor(max_workers=5) as pool:
            writers = [pool.submit(writer, worker) for worker in range(4)]
            ddl = pool.submit(drop)
            results = [future.result(timeout=180) for future in writers]
            ddl.result(timeout=90)
        acknowledged = [seed] + [row for rows, _ in results for row in rows]
        errors = [error for _, failures in results for error in failures]
        model = {row["id"]: copy.deepcopy(row) for row in acknowledged}
        self._check(client, name, model)
        self._reload(client, name)
        self._check(client, name, model)
        allure.attach(
            str(errors), name="insert rejections during child drop", attachment_type=allure.attachment_type.TEXT
        )
        if errors:
            assert all(is_subfield_drop_rejection(e["code"], e["message"]) for e in errors), errors
            raise ConcurrentSubfieldDropRejection(f"{SUBFIELD_DROP_ISSUE}: {errors}")
