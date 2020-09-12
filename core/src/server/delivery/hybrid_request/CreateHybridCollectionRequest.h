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
#include <utility>
#include <vector>

#include "server/delivery/request/BaseRequest.h"

namespace milvus {
namespace server {

class CreateHybridCollectionRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           std::vector<std::pair<std::string, engine::meta::hybrid::DataType>>& field_types,
           std::vector<std::pair<std::string, uint64_t>>& vector_dimensions,
           std::vector<std::pair<std::string, std::string>>& field_params);

 protected:
    CreateHybridCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                  const std::string& collection_name,
                                  std::vector<std::pair<std::string, engine::meta::hybrid::DataType>>& field_types,
                                  std::vector<std::pair<std::string, uint64_t>>& vector_dimensions,
                                  std::vector<std::pair<std::string, std::string>>& field_arams);

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    std::vector<std::pair<std::string, engine::meta::hybrid::DataType>>& field_types_;
    std::vector<std::pair<std::string, uint64_t>> vector_dimensions_;
    std::vector<std::pair<std::string, std::string>>& field_params_;
};

}  // namespace server
}  // namespace milvus
