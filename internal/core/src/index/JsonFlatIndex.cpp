// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "index/JsonFlatIndex.h"
#include "common/Types.h"
#include "index/InvertedIndexUtil.h"
namespace milvus::index {

void
JsonFlatIndex::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int i = 0; i < n; i++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset.push_back(i);
                continue;
            }
            auto json = static_cast<const Json*>(data->RawValue(i));
            wrapper_->add_json_data(json, 1, offset++);
        }
    }
}
}  // namespace milvus::index
