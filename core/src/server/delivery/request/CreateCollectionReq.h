// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "server/delivery/request/BaseReq.h"

namespace milvus {
namespace server {

class CreateCollectionReq : public BaseReq {
 public:
    static BaseReqPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           std::unordered_map<std::string, engine::meta::DataType>& field_types,
           std::unordered_map<std::string, milvus::json>& field_index_params,
           std::unordered_map<std::string, std::string>& field_params, milvus::json& extra_params);

 protected:
    CreateCollectionReq(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                        std::unordered_map<std::string, engine::meta::DataType>& field_types,
                        std::unordered_map<std::string, milvus::json>& field_index_params,
                        std::unordered_map<std::string, std::string>& field_params, milvus::json& extra_params);

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    std::unordered_map<std::string, engine::meta::DataType> field_types_;
    std::unordered_map<std::string, milvus::json> field_index_params_;
    std::unordered_map<std::string, std::string> field_params_;
    milvus::json extra_params_;
};

}  // namespace server
}  // namespace milvus
