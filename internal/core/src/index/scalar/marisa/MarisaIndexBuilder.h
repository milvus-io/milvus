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

#include <string_view>

#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "storage/artifact/Artifact.h"

// Marisa builder. Builds the trie from stable borrowed input, then traverses
// that same input again to fill string IDs without copying the raw strings.

namespace milvus::index {

class MarisaIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<std::string_view>> {
 public:
    explicit MarisaIndexBuilder(DataType value_type);

    ~MarisaIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<std::string_view>& input) &&
        override;

 private:
    DataType value_type_;
};

}  // namespace milvus::index
