// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>

#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/vector/KnowhereEngine.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {

class VectorDiskBuildFileManager;

// One-shot disk-vector builder. The caller owns an immutable prepared input
// generation for the complete synchronous Build call. The returned artifact
// owns only the distinct builder-output generation.
template <typename T>
class VectorDiskBuilder final
    : public IArtifactBuilder<PreparedVectorBuildFiles<T>> {
 public:
    VectorDiskBuilder(DataType elem_type,
                      IndexType index_type,
                      MetricType metric_type,
                      IndexVersion version,
                      int64_t dim,
                      knowhere::Json build_params,
                      std::string local_dir);

    ~VectorDiskBuilder() override = default;

    BuilderInputSpec
    InputSpec() const override;

    storage::ArtifactPtr
        Build(const PreparedVectorBuildFiles<T>& input) &&
        override;

 private:
    void
    EnsureOpen(const char* operation) const;

    std::shared_ptr<storage::LocalDirectory> local_files_;
    std::shared_ptr<VectorDiskBuildFileManager> file_manager_;
    std::unique_ptr<KnowhereEngine> engine_;
    knowhere::Json build_params_;
    bool failed_{false};
    bool sealed_{false};
};

}  // namespace milvus::index
