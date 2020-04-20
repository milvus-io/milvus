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

#include "MilvusApi.h"
#include "BooleanQuery.h"
#include "thirdparty/nlohmann/json.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

using JSON = nlohmann::json;

namespace milvus_sdk {

class Utils {
 public:
    static std::string
    CurrentTime();

    static std::string
    CurrentTmDate(int64_t offset_day = 0);

    static const std::string&
    GenCollectionName();

    static void
    Sleep(int seconds);

    static std::string
    MetricTypeName(const milvus::MetricType& metric_type);

    static std::string
    IndexTypeName(const milvus::IndexType& index_type);

    static void
    PrintCollectionParam(const milvus::CollectionParam& collection_param);

    static void
    PrintPartitionParam(const milvus::PartitionParam& partition_param);

    static void
    PrintIndexParam(const milvus::IndexParam& index_param);

    static void
    BuildEntities(int64_t from, int64_t to, std::vector<milvus::Entity>& entity_array,
                  std::vector<int64_t>& entity_ids, int64_t dimension);

    static void
    PrintSearchResult(const std::vector<std::pair<int64_t, milvus::Entity>>& entity_array,
                      const milvus::TopKQueryResult& topk_query_result);

    static void
    CheckSearchResult(const std::vector<std::pair<int64_t, milvus::Entity>>& entity_array,
                      const milvus::TopKQueryResult& topk_query_result);

    static void
    DoSearch(std::shared_ptr<milvus::Connection> conn, const std::string& collection_name,
             const std::vector<std::string>& partition_tags, int64_t top_k, int64_t nprobe,
             const std::vector<std::pair<int64_t, milvus::Entity>>& entity_array,
             milvus::TopKQueryResult& topk_query_result);

    static void
    PrintCollectionInfo(const milvus::CollectionInfo& collection_info);

    static std::vector<milvus::LeafQueryPtr>
    GenLeafQuery();
};

}  // namespace milvus_sdk
