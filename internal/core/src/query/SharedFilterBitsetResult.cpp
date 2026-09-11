// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "query/SharedFilterBitsetResult.h"

#include "exec/QueryContext.h"

namespace milvus::query {

void
SharedFilterBitsetResult::CaptureFrom(const milvus::exec::QueryContext& ctx) {
    all_rows_visible = ctx.get_all_rows_visible();
    bitset_is_element_level = ctx.bitset_is_element_level();
    active_element_count = ctx.get_active_element_count();
    array_offsets = ctx.get_array_offsets();
    struct_name = ctx.get_struct_name();
}

void
SharedFilterBitsetResult::ApplyTo(milvus::exec::QueryContext& ctx) const {
    ctx.set_all_rows_visible(all_rows_visible);
    ctx.set_bitset_is_element_level(bitset_is_element_level);
    ctx.set_active_element_count(active_element_count);
    ctx.set_array_offsets(array_offsets);
    ctx.set_struct_name(struct_name);
}

}  // namespace milvus::query
