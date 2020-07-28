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
#include "server/delivery/request/BaseReq.h"

namespace milvus {
namespace server {

class CreateIndexRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           const std::string& field_name, const std::string& index_name, const milvus::json& json_params);

 protected:
    CreateIndexRequest(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                       const std::string& field_name, const std::string& index_name, const milvus::json& json_params);

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    const std::string field_name_;
    const std::string index_name_;
    const milvus::json json_params_;
};

}  // namespace server
}  // namespace milvus
