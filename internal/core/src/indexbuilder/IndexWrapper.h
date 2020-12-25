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

#include <string>
#include <optional>
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace indexbuilder {

class IndexWrapper {
 public:
    explicit IndexWrapper(const char* serialized_type_params, const char* serialized_index_params);

    int64_t
    dim();

    void
    BuildWithoutIds(const knowhere::DatasetPtr& dataset);

    struct Binary {
        char* data;
        int32_t size;
    };

    Binary
    Serialize();

    void
    Load(const char* serialized_sliced_blob_buffer, int32_t size);

 private:
    void
    parse();

    template <typename T>
    std::optional<T>
    get_config_by_name(std::string name);

 private:
    knowhere::VecIndexPtr index_ = nullptr;
    std::string type_params_;
    std::string index_params_;
    milvus::json type_config_;
    milvus::json index_config_;
    knowhere::Config config_;
};

}  // namespace indexbuilder
}  // namespace milvus
