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

#include "server/delivery/request/BaseRequest.h"

#include <src/context/HybridSearchContext.h>
#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

class HybridSearchRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           std::vector<std::string>& partition_list, query::GeneralQueryPtr& general_query, query::QueryPtr& query_ptr,
           milvus::json& json_params, std::vector<std::string>& field_names, engine::QueryResult& result);

 protected:
    HybridSearchRequest(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                        std::vector<std::string>& partition_list, query::GeneralQueryPtr& general_query,
                        query::QueryPtr& query_ptr, milvus::json& json_params, std::vector<std::string>& field_names,
                        engine::QueryResult& result);

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    std::vector<std::string> partition_list_;
    milvus::query::GeneralQueryPtr general_query_;
    milvus::query::QueryPtr query_ptr_;
    milvus::json json_params_;
    std::vector<std::string>& field_names_;
    engine::QueryResult& result_;
};

}  // namespace server
}  // namespace milvus
