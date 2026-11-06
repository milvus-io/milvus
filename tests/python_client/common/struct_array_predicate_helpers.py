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

"""Shared setup, data models and assertions for struct document contracts."""

from common.struct_array_nullable_helpers import (
    _record,
)


def _not(value):
    return None if value is None else not value


def _and(left, right):
    return False if left is False or right is False else None if left is None or right is None else True


def _or(left, right):
    return True if left is True or right is True else None if left is None or right is None else False


def _positive(element):
    return None if element["score"] is None else element["score"] > 0


def _expression_rows():
    shapes = [
        None,
        [],
        [(None, None)],
        [(1, [])],
        [(-1, [None])],
        [(0, [1, None])],
        [(1, [1, 2])],
        [(None, []), (1, [2])],
        [(-1, [2]), (None, None)],
        [(1, [1]), (-1, [])],
        [(None, [None]), (None, [1, None])],
    ]
    return [
        _record(
            n,
            None
            if shape is None
            else [
                {"anchor": j, "score": score, "label": None, "emb": None, "tags": tags}
                for j, (score, tags) in enumerate(shape)
            ],
        )
        for n, shape in enumerate(shapes)
    ]
