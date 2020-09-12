// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "server/delivery/request/BaseRequest.h"

namespace milvus {
namespace server {

class SearchByIDRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           const std::vector<int64_t>& id_array, int64_t topk, const milvus::json& extra_params,
           const std::vector<std::string>& partition_list, TopKQueryResult& result);

 protected:
    SearchByIDRequest(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                      const std::vector<int64_t>& id_array, int64_t topk, const milvus::json& extra_params,
                      const std::vector<std::string>& partition_list, TopKQueryResult& result);

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    std::vector<int64_t> id_array_;
    int64_t topk_;
    milvus::json extra_params_;
    const std::vector<std::string> partition_list_;

    TopKQueryResult& result_;
};

}  // namespace server
}  // namespace milvus
