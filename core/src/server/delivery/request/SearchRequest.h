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

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

class SearchRequest : public BaseRequest {
 public:
    static BaseRequestPtr
    Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
           const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
           const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
           TopKQueryResult& result);

    const std::string&
    CollectionName() const {
        return collection_name_;
    }

    const engine::VectorsData&
    VectorsData() const {
        return vectors_data_;
    }

    int64_t
    TopK() const {
        return topk_;
    }

    const milvus::json&
    ExtraParams() const {
        return extra_params_;
    }

    const std::vector<std::string>&
    PartitionList() const {
        return partition_list_;
    }

    const std::vector<std::string>&
    FileIDList() const {
        return file_id_list_;
    }

    TopKQueryResult&
    QueryResult() {
        return result_;
    }

    const milvus::engine::meta::CollectionSchema&
    TableSchema() const {
        return collection_schema_;
    }

 protected:
    SearchRequest(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                  const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
                  const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
                  TopKQueryResult& result);

    Status
    OnPreExecute() override;

    Status
    OnExecute() override;

 private:
    const std::string collection_name_;
    const engine::VectorsData vectors_data_;
    int64_t topk_;
    milvus::json extra_params_;
    const std::vector<std::string> partition_list_;
    const std::vector<std::string> file_id_list_;

    TopKQueryResult& result_;

    // for validation
    milvus::engine::meta::CollectionSchema collection_schema_;
};

using SearchRequestPtr = std::shared_ptr<SearchRequest>;

}  // namespace server
}  // namespace milvus
