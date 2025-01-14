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

#pragma once

#include <marisa.h>
#include "index/StringIndex.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

class StringIndexMarisa : public StringIndex {
 public:
    explicit StringIndexMarisa(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    int64_t
    Size() override;

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& set, const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    int64_t
    Count() override {
        return str_ids_.size();
    }

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::MARISA;
    }

    void
    Build(size_t n,
          const std::string* values,
          const bool* valid_data = nullptr) override;

    void
    Build(const Config& config = {}) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& field_datas) override;

    const TargetBitmap
    In(size_t n, const std::string* values) override;

    const TargetBitmap
    NotIn(size_t n, const std::string* values) override;

    const TargetBitmap
    IsNull() override;

    const TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    Range(std::string value, OpType op) override;

    const TargetBitmap
    Range(std::string lower_bound_value,
          bool lb_inclusive,
          std::string upper_bound_value,
          bool ub_inclusive) override;

    const TargetBitmap
    PrefixMatch(const std::string_view prefix) override;

    std::optional<std::string>
    Reverse_Lookup(size_t offset) const override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        return true;
    }

 private:
    void
    fill_str_ids(size_t n, const std::string* values, const bool* valid_data);

    void
    fill_offsets();

    // get str_id by str, if str not found, -1 was returned.
    size_t
    lookup(const std::string_view str);

    std::vector<size_t>
    prefix_match(const std::string_view prefix);

    bool
    in_lexicographic_order();

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

 private:
    Config config_;
    marisa::Trie trie_;
    std::vector<int64_t> str_ids_;  // used to retrieve.
    std::map<size_t, std::vector<size_t>> str_ids_to_offsets_;
    bool built_ = false;
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
};

using StringIndexMarisaPtr = std::unique_ptr<StringIndexMarisa>;

inline StringIndexPtr
CreateStringIndexMarisa(
    const storage::FileManagerContext& file_manager_context =
        storage::FileManagerContext()) {
    return std::make_unique<StringIndexMarisa>(file_manager_context);
}
}  // namespace milvus::index
