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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Array.h"
#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/sort/IndexStructure.h"
#include "storage/artifact/Artifact.h"

namespace milvus::index {

struct SortedBuildParams {
    bool nested{false};
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
};

template <typename T>
class SortedIndexBuilder final : public IArtifactBuilder<ScalarBuildInput<T>> {
 public:
    explicit SortedIndexBuilder(SortedBuildParams params);

    ~SortedIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<T>& input) &&
        override;

 private:
    SortedBuildParams params_;
    std::vector<IndexStructure<T>> data_;
    TargetBitmap validity_;
    size_t total_num_rows_{0};
};

class SortedStringIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<std::string_view>> {
 public:
    explicit SortedStringIndexBuilder(SortedBuildParams params);

    ~SortedStringIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<std::string_view>& input) &&
        override;

 private:
    SortedBuildParams params_;

    std::map<std::string, std::vector<uint32_t>> postings_;
    TargetBitmap validity_;
    size_t total_num_rows_{0};
};

// Ordinary ARRAY input keeps row coordinates: every element in one ArrayView
// points to the same row, while valid empty rows, null rows, and duplicate
// elements still preserve that row's validity/identity. Nested callers flatten
// first and use the typed builders above.
class SortedArrayIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<ArrayView>> {
 public:
    class Impl;

    explicit SortedArrayIndexBuilder(SortedBuildParams params);
    ~SortedArrayIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<ArrayView>& input) &&
        override;

 private:
    SortedBuildParams params_;
    std::unique_ptr<Impl> impl_;
    TargetBitmap validity_;
    size_t total_num_rows_{0};
};

}  // namespace milvus::index
