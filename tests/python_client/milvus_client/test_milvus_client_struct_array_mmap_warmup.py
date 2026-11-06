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

"""Struct array mmap warmup regression cases."""

import pytest
from common.common_type import CaseLabel
from common.struct_array_typed_vector_helpers import StructArrayTypedVectorHelpersBase
from pymilvus import DataType
from utils.util_log import test_log as log


class TestMilvusClientStructArrayMmapWarmup(StructArrayTypedVectorHelpersBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mmap", [None, "true", "false"], ids=["default", "on", "off"])
    @pytest.mark.parametrize("warmup", [None, "disable", "sync", "async"], ids=["default", "disable", "sync", "async"])
    def test_parent_mmap_and_warmup_preserve_rows_after_reload(self, mmap, warmup):
        """
        target: retain nullable values and search results across parent mmap/warmup settings
        method: sealed shared schema, configured load, first/repeated reads and second load
        expected: independent complete rows and element scores; runtime path evidence is separate
        """
        client = self._client()
        dtype = DataType.FLOAT_VECTOR
        name = self._typed_collection(client, dtype, "FLAT", "L2")
        model = self._seed(client, name, self._typed_rows(dtype), split=False)
        self.flush(client, name)
        self.release_collection(client, name)
        properties = {}
        if mmap is not None:
            properties["mmap.enabled"] = mmap
        if warmup is not None:
            properties["warmup"] = warmup
        if properties:
            self.alter_collection_field(client, name, field_name="clips", field_params=properties)
        log.info(f"STRUCT_CONFIG collection={name} parent_properties={properties}")
        self.load_collection(client, name)
        for phase in range(3):
            self._typed_check(client, name, model, dtype)
            self._typed_search(client, name, model, dtype, "FLAT", "L2")
            if phase == 1:
                self.release_collection(client, name)
                self.load_collection(client, name)
