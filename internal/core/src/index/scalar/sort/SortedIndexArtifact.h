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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/scalar/sort/SortedIndexReader.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// Numeric and string artifacts remain separate because their persisted layouts
// differ.

namespace milvus::index {

template <typename T>
class SortedIndexArtifact final : public storage::Artifact {
 public:
    SortedIndexArtifact(std::vector<IndexStructure<T>> data,
                        TargetBitmap valid_bitset,
                        std::vector<int32_t> idx_to_offsets,
                        size_t total_num_rows,
                        bool nested);

    ~SortedIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::vector<IndexStructure<T>> data_;
    TargetBitmap valid_bitset_;
    std::vector<int32_t> idx_to_offsets_;
    size_t total_num_rows_{0};
    bool nested_{false};
};

class SortedStringIndexArtifact final : public storage::Artifact {
 public:
    SortedStringIndexArtifact(std::vector<std::string> unique_values,
                              std::vector<std::vector<uint32_t>> posting_lists,
                              TargetBitmap valid_bitset,
                              std::vector<int32_t> idx_to_offsets,
                              size_t total_num_rows,
                              bool nested);

    ~SortedStringIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<const SortedStringLayout> layout_;
    TargetBitmap valid_bitset_;
    std::vector<int32_t> idx_to_offsets_;
    size_t total_num_rows_{0};
    bool nested_{false};
};

}  // namespace milvus::index
