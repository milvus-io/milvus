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
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/ngram/JsonProjectedString.h"
#include "storage/artifact/Artifact.h"

// Decoded batches from the complete input feed the Tantivy writer directly.

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::index {

class NgramBuilderCore;

struct NgramBuildParams {
    std::string field_name;
    DataType value_type{DataType::VARCHAR};
    uintptr_t min_gram{0};
    uintptr_t max_gram{0};
    std::string local_dir;
};

// std::string_view input carries scalar validity. JsonProjectedString keeps
// field-null, missing projection, and present values distinct while sharing
// the same NGRAM writer core.
template <typename T>
class NgramIndexBuilder final : public IArtifactBuilder<ScalarBuildInput<T>> {
 public:
    static_assert(std::is_same_v<T, std::string_view> ||
                  std::is_same_v<T, JsonProjectedString>);

    explicit NgramIndexBuilder(NgramBuildParams params);

    ~NgramIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<T>& input) &&
        override;

 private:
    std::unique_ptr<NgramBuilderCore> core_;
};

}  // namespace milvus::index
