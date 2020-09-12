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
    Create(const std::shared_ptr<milvus::server::Context>& context,
           context::HybridSearchContextPtr& hybrid_search_context, const std::string& collection_name,
           std::vector<std::string>& partition_list, milvus::query::GeneralQueryPtr& general_query,
           TopKQueryResult& result);

 protected:
    HybridSearchRequest(const std::shared_ptr<milvus::server::Context>& context,
                        context::HybridSearchContextPtr& hybrid_search_context, const std::string& collection_name,
                        std::vector<std::string>& partition_list, milvus::query::GeneralQueryPtr& general_query,
                        TopKQueryResult& result);

    Status
    OnExecute() override;

 private:
    context::HybridSearchContextPtr hybrid_search_contxt_;
    const std::string collection_name_;
    std::vector<std::string>& partition_list_;
    milvus::query::GeneralQueryPtr& general_query_;
    TopKQueryResult& result_;
};

}  // namespace server
}  // namespace milvus
