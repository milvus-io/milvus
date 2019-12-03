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

#include "MilvusApi.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace milvus_sdk {

class Utils {
 public:
    static std::string
    CurrentTime();

    static std::string
    CurrentTmDate(int64_t offset_day = 0);

    static const std::string&
    GenTableName();

    static void
    Sleep(int seconds);

    static std::string
    MetricTypeName(const milvus::MetricType& metric_type);

    static std::string
    IndexTypeName(const milvus::IndexType& index_type);

    static void
    PrintTableSchema(const milvus::TableSchema& tb_schema);

    static void
    PrintPartitionParam(const milvus::PartitionParam& partition_param);

    static void
    PrintIndexParam(const milvus::IndexParam& index_param);

    static void
    BuildVectors(int64_t from, int64_t to, std::vector<milvus::RowRecord>& vector_record_array,
                 std::vector<int64_t>& record_ids, int64_t dimension);

    static void
    PrintSearchResult(const std::vector<std::pair<int64_t, milvus::RowRecord>>& search_record_array,
                      const milvus::TopKQueryResult& topk_query_result);

    static void
    CheckSearchResult(const std::vector<std::pair<int64_t, milvus::RowRecord>>& search_record_array,
                      const milvus::TopKQueryResult& topk_query_result);

    static void
    DoSearch(std::shared_ptr<milvus::Connection> conn, const std::string& table_name,
             const std::vector<std::string>& partition_tags, int64_t top_k, int64_t nprobe,
             const std::vector<std::pair<int64_t, milvus::RowRecord>>& search_record_array,
             milvus::TopKQueryResult& topk_query_result);
};

}  // namespace milvus_sdk
