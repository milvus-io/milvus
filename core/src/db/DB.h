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
#include <vector>

#include "Options.h"
#include "Types.h"
#include "meta/Meta.h"
#include "server/context/Context.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class Env;

class DB {
 public:
    DB() = default;
    DB(const DB&) = delete;
    DB&
    operator=(const DB&) = delete;

    virtual ~DB() = default;

    virtual Status
    Start() = 0;
    virtual Status
    Stop() = 0;

    virtual Status
    CreateTable(meta::TableSchema& table_schema_) = 0;

    virtual Status
    DropTable(const std::string& table_id) = 0;

    virtual Status
    DescribeTable(meta::TableSchema& table_schema_) = 0;

    virtual Status
    HasTable(const std::string& table_id, bool& has_or_not_) = 0;

    virtual Status
    AllTables(std::vector<meta::TableSchema>& table_schema_array) = 0;

    virtual Status
    GetTableInfo(const std::string& table_id, TableInfo& table_info) = 0;

    virtual Status
    GetTableRowCount(const std::string& table_id, uint64_t& row_count) = 0;

    virtual Status
    PreloadTable(const std::string& table_id) = 0;

    virtual Status
    UpdateTableFlag(const std::string& table_id, int64_t flag) = 0;

    virtual Status
    CreatePartition(const std::string& table_id, const std::string& partition_name,
                    const std::string& partition_tag) = 0;

    virtual Status
    DropPartition(const std::string& partition_name) = 0;

    virtual Status
    DropPartitionByTag(const std::string& table_id, const std::string& partition_tag) = 0;

    virtual Status
    ShowPartitions(const std::string& table_id, std::vector<meta::TableSchema>& partition_schema_array) = 0;

    virtual Status
    InsertVectors(const std::string& table_id, const std::string& partition_tag, VectorsData& vectors) = 0;

    virtual Status
    DeleteVector(const std::string& table_id, IDNumber vector_id) = 0;

    virtual Status
    DeleteVectors(const std::string& table_id, IDNumbers vector_ids) = 0;

    virtual Status
    Flush(const std::string& table_id) = 0;

    virtual Status
    Flush() = 0;

    virtual Status
    Compact(const std::string& table_id) = 0;

    virtual Status
    GetVectorByID(const std::string& table_id, const IDNumber& vector_id, VectorsData& vector) = 0;

    //    virtual Status
    //    Merge(const std::set<std::string>& table_ids) = 0;

    virtual Status
    QueryByID(const std::shared_ptr<server::Context>& context, const std::string& table_id,
              const std::vector<std::string>& partition_tags, uint64_t k, uint64_t nprobe, IDNumber vector_id,
              ResultIds& result_ids, ResultDistances& result_distances) = 0;

    virtual Status
    Query(const std::shared_ptr<server::Context>& context, const std::string& table_id,
          const std::vector<std::string>& partition_tags, uint64_t k, uint64_t nprobe, const VectorsData& vectors,
          ResultIds& result_ids, ResultDistances& result_distances) = 0;

    virtual Status
    QueryByFileID(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                  const std::vector<std::string>& file_ids, uint64_t k, uint64_t nprobe, const VectorsData& vectors,
                  ResultIds& result_ids, ResultDistances& result_distances) = 0;

    virtual Status
    Size(uint64_t& result) = 0;

    virtual Status
    CreateIndex(const std::string& table_id, const TableIndex& index) = 0;

    virtual Status
    DescribeIndex(const std::string& table_id, TableIndex& index) = 0;

    virtual Status
    DropIndex(const std::string& table_id) = 0;

    virtual Status
    DropAll() = 0;
};  // DB

using DBPtr = std::shared_ptr<DB>;

}  // namespace engine
}  // namespace milvus
