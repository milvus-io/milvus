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

#pragma once

#include "indexbuilder/IndexCreatorBase.h"
#include "pb/index_cgo_msg.pb.h"
#include <string>
#include <memory>
#include <common/CDataType.h>
#include "index/Index.h"
#include "index/ScalarIndex.h"

namespace milvus::indexbuilder {

class ScalarIndexCreator : public IndexCreatorBase {
 public:
    ScalarIndexCreator(CDataType dtype, const char* type_params, const char* index_params);

    void
    Build(const knowhere::DatasetPtr& dataset) override;

    knowhere::BinarySet
    Serialize() override;

    void
    Load(const knowhere::BinarySet&) override;

 private:
    std::string
    index_type();

    void
    parseConfig();

 private:
    scalar::IndexBasePtr index_ = nullptr;
    proto::indexcgo::TypeParams type_params_;
    proto::indexcgo::IndexParams index_params_;
    knowhere::Config config_;
    CDataType dtype_;
};

using ScalarIndexCreatorPtr = std::unique_ptr<ScalarIndexCreator>;

inline ScalarIndexCreatorPtr
CreateScalarIndex(CDataType dtype, const char* type_params, const char* index_params) {
    return std::make_unique<ScalarIndexCreator>(dtype, type_params, index_params);
}

}  // namespace milvus::indexbuilder
