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

"""Shared setup, data models and assertions for struct array wire contracts."""

import copy


def _fields(request):
    for field in request.fields_data:
        if field.WhichOneof("field") == "struct_arrays":
            for child in field.struct_arrays.fields:
                yield f"{field.field_name}[{child.field_name}]", child
        else:
            yield field.field_name, field


def _set_masks(field, legacy, current):
    wrapper = getattr(field, field.WhichOneof("field"))
    del field.valid_data[:]
    field.valid_data.extend(legacy)
    del wrapper.valid_data[:]
    wrapper.valid_data.extend(current)


def _encode_masks(request, location, template=None):
    changed = []
    template_fields = dict(_fields(template)) if template is not None else {}
    for path, field in _fields(request):
        if field.WhichOneof("field") is None:
            # The SDK omits all-null wrappers. Retain schema metadata from a
            # non-null encoding, while removing every actual value and validity bit.
            source = template_fields[path]
            kind = source.WhichOneof("field")
            empty = copy.deepcopy(getattr(source, kind))
            getattr(empty, empty.WhichOneof("data")).ClearField("data")
            empty.ClearField("valid_data")
            getattr(field, kind).CopyFrom(empty)
        wrapper = getattr(field, field.WhichOneof("field"))
        mask = list(field.valid_data) or list(wrapper.valid_data)
        if mask:
            _set_masks(field, mask if location != "current" else [], mask if location != "legacy" else [])
            changed.append(path)
    assert {"note", "clips[value]", "clips[tag]", "clips[flag]", "clips[emb]"}.issubset(changed)
    return request
