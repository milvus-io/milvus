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
#if 0
#include "IndexMeta.h"
#include <mutex>
#include <cassert>
namespace milvus::segcore {

Status
IndexMeta::AddEntry(
    const std::string& index_name, const std::string& field_name_, IndexType type, IndexMode mode, IndexConfig config) {
    auto field_name = FieldName(field_name_);
    Entry entry{index_name, field_name, type, mode, std::move(config)};
    VerifyEntry(entry);

    if (entries_.count(index_name)) {
        throw std::invalid_argument("duplicate index_name");
    }
    // TODO: support multiple indexes for single field
    Assert(!lookups_.count(field_name));
    lookups_[field_name] = index_name;
    entries_[index_name] = std::move(entry);

    return Status::OK();
}

Status
IndexMeta::DropEntry(const std::string& index_name) {
    Assert(entries_.count(index_name));
    auto entry = std::move(entries_[index_name]);
    if (lookups_[entry.field_name] == index_name) {
        lookups_.erase(entry.field_name);
    }
    return Status::OK();
}

void
IndexMeta::VerifyEntry(const Entry& entry) {
    auto is_mode_valid = std::set{IndexMode::MODE_CPU, IndexMode::MODE_GPU}.count(entry.mode);
    if (!is_mode_valid) {
        throw std::invalid_argument("invalid mode");
    }

    auto& schema = *schema_;
    auto& field_meta = schema[entry.field_name];
    // TODO checking
    if (field_meta.is_vector()) {
        Assert(entry.type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ);
    } else {
        Assert(false);
    }
}

}  // namespace milvus::segcore

#endif
