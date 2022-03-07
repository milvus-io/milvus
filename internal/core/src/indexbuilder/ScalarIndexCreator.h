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

#include "indexbuilder/ScalarIndexCreatorBase.h"
#include "knowhere/index/structured_index_simple/StructuredIndex.h"
#include "pb/index_cgo_msg.pb.h"
#include <string>
#include <memory>

namespace milvus::indexbuilder {

template <typename T>
class ScalarIndexCreator : public ScalarIndexCreatorBase {
    // of course, maybe we can support combination index later.
    // for example, we can create index for combination of (field a, field b),
    // attribute filtering on the combination can be speed up.
    static_assert(std::is_fundamental_v<T> || std::is_same_v<T, std::string>);

 public:
    ScalarIndexCreator(const char* type_params, const char* index_params);

    void
    Build(const knowhere::DatasetPtr& dataset) override;

    std::unique_ptr<knowhere::BinarySet>
    Serialize() override;

 private:
    knowhere::scalar::StructuredIndexPtr<T> index_ = nullptr;
    proto::indexcgo::MapParams type_params_;
    proto::indexcgo::MapParams index_params_;
    milvus::json config_;
};
}  // namespace milvus::indexbuilder

#include "ScalarIndexCreator-inl.h"
