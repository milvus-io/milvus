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
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace indexbuilder {

class IndexWrapper {
 public:
    explicit IndexWrapper(const char* type_params_str, const char* index_params_str);

    int64_t
    dim();

    void
    BuildWithoutIds(const knowhere::DatasetPtr& dataset);

    char*
    Serialize();

    void
    Load(const char* dumped_blob_buffer);

 private:
    void
    parse();

 private:
    knowhere::VecIndexPtr index_ = nullptr;
    std::string type_params_;
    std::string index_params_;
    milvus::Json type_config_;
    milvus::Json index_config_;
    knowhere::Config config_;
};

}  // namespace indexbuilder
}  // namespace milvus
