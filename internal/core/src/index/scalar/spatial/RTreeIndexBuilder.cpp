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

#include "index/scalar/spatial/RTreeIndexBuilder.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "index/scalar/spatial/RTreeIndexParams.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/scalar/spatial/RTreeIndexArtifact.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {
namespace {

using spatial_params::ValidateGeometryParams;

RTreeBuildParams
ParseBuildParams(const BuildParams& params) {
    ValidateGeometryParams(params);
    RTreeBuildParams result;
    if (params.contains("local_dir") && !params.at("local_dir").is_null()) {
        if (!params.at("local_dir").is_string()) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter local_dir must be a string");
        }
        result.local_dir = params.at("local_dir").get<std::string>();
    }
    return result;
}

}  // namespace

RTreeIndexBuilder::RTreeIndexBuilder(RTreeBuildParams params)
    : directory_(CreateRTreeIndexDirectory(params.local_dir, {})),
      engine_(std::make_unique<RTreeBuildEngine>(
          (std::filesystem::path(directory_->Path()) / "index_file").string())) {
}

RTreeIndexBuilder::~RTreeIndexBuilder() = default;

void
RTreeIndexBuilder::AddBatch(const ScalarBuildBatch<std::string_view>& batch) {
    const auto n = batch.values.size();
    if (n > static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
        total_num_rows_ >
            std::numeric_limits<int64_t>::max() - static_cast<int64_t>(n)) {
        ThrowInfo(DataTypeInvalid,
                  "R-Tree coordinate count {} + {} exceeds int64 domain",
                  total_num_rows_,
                  n);
    }

    for (size_t i = 0; i < n; ++i) {
        const auto row = total_num_rows_ + static_cast<int64_t>(i);
        if (batch.validity && !batch.validity[i]) {
            null_offsets_.push_back(static_cast<size_t>(row));
            continue;
        }
        engine_->AddGeometry(
            reinterpret_cast<const uint8_t*>(batch.values[i].data()),
            batch.values[i].size(),
            row);
    }
    total_num_rows_ += static_cast<int64_t>(n);
}

storage::ArtifactPtr
RTreeIndexBuilder::Build(const ScalarBuildInput<std::string_view>& input) && {
    for (const auto& batch : input.batches) {
        AddBatch(batch);
    }
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "R-Tree index cannot build empty input");
    }
    engine_->Finish();
    auto artifact = std::make_unique<RTreeIndexArtifact>(
        directory_, std::move(null_offsets_));
    engine_.reset();
    directory_.reset();
    return artifact;
}

namespace {

const bool kRTreeBuilderRegistered = [] {
    BuilderRegistry<ScalarBuildInput<std::string_view>>::Instance().Register(
        families::kRTree, [](const BuildParams& params) {
            return std::make_unique<RTreeIndexBuilder>(
                ParseBuildParams(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
