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

#include "MetaTypes.h"
#include "db/Options.h"
#include "db/Types.h"
#include "utils/Status.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace engine {
namespace meta {

static const char* META_TABLES = "Tables";
static const char* META_TABLEFILES = "TableFiles";

class Meta {
 public:
    class CleanUpFilter {
     public:
        virtual bool
        IsIgnored(const TableFileSchema& schema) = 0;
    };

 public:
    virtual ~Meta() = default;

    virtual Status
    CreateTable(TableSchema& table_schema) = 0;

    virtual Status
    DescribeTable(TableSchema& table_schema) = 0;

    virtual Status
    HasTable(const std::string& table_id, bool& has_or_not) = 0;

    virtual Status
    AllTables(std::vector<TableSchema>& table_schema_array) = 0;

    virtual Status
    UpdateTableFlag(const std::string& table_id, int64_t flag) = 0;

    virtual Status
    DropTable(const std::string& table_id) = 0;

    virtual Status
    DeleteTableFiles(const std::string& table_id) = 0;

    virtual Status
    CreateTableFile(TableFileSchema& file_schema) = 0;

    virtual Status
    DropDataByDate(const std::string& table_id, const DatesT& dates) = 0;

    virtual Status
    GetTableFiles(const std::string& table_id, const std::vector<size_t>& ids, TableFilesSchema& table_files) = 0;

    virtual Status
    UpdateTableFile(TableFileSchema& file_schema) = 0;

    virtual Status
    UpdateTableFiles(TableFilesSchema& files) = 0;

    virtual Status
    UpdateTableIndex(const std::string& table_id, const TableIndex& index) = 0;

    virtual Status
    UpdateTableFilesToIndex(const std::string& table_id) = 0;

    virtual Status
    DescribeTableIndex(const std::string& table_id, TableIndex& index) = 0;

    virtual Status
    DropTableIndex(const std::string& table_id) = 0;

    virtual Status
    CreatePartition(const std::string& table_name, const std::string& partition_name, const std::string& tag) = 0;

    virtual Status
    DropPartition(const std::string& partition_name) = 0;

    virtual Status
    ShowPartitions(const std::string& table_name, std::vector<meta::TableSchema>& partition_schema_array) = 0;

    virtual Status
    GetPartitionName(const std::string& table_name, const std::string& tag, std::string& partition_name) = 0;

    virtual Status
    FilesToSearch(const std::string& table_id, const std::vector<size_t>& ids, const DatesT& dates,
                  DatePartionedTableFilesSchema& files) = 0;

    virtual Status
    FilesToMerge(const std::string& table_id, DatePartionedTableFilesSchema& files) = 0;

    virtual Status
    FilesToIndex(TableFilesSchema&) = 0;

    virtual Status
    FilesByType(const std::string& table_id, const std::vector<int>& file_types, TableFilesSchema& table_files) = 0;

    virtual Status
    Size(uint64_t& result) = 0;

    virtual Status
    Archive() = 0;

    virtual Status
    CleanUpShadowFiles() = 0;

    virtual Status
    CleanUpFilesWithTTL(uint64_t seconds, CleanUpFilter* filter = nullptr) = 0;

    virtual Status
    DropAll() = 0;

    virtual Status
    Count(const std::string& table_id, uint64_t& result) = 0;
};  // MetaData

using MetaPtr = std::shared_ptr<Meta>;

}  // namespace meta
}  // namespace engine
}  // namespace milvus
