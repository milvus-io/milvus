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

#include <mutex>
#include <string>
#include <vector>

#include "Meta.h"
#include "db/Options.h"

namespace milvus {
namespace engine {
namespace meta {

auto
StoragePrototype(const std::string& path);

class SqliteMetaImpl : public Meta {
 public:
    explicit SqliteMetaImpl(const DBMetaOptions& options);
    ~SqliteMetaImpl();

    Status
    CreateTable(TableSchema& table_schema) override;

    Status
    DescribeTable(TableSchema& table_schema) override;

    Status
    HasTable(const std::string& table_id, bool& has_or_not) override;

    Status
    AllTables(std::vector<TableSchema>& table_schema_array) override;

    Status
    DropTable(const std::string& table_id) override;

    Status
    DeleteTableFiles(const std::string& table_id) override;

    Status
    CreateTableFile(TableFileSchema& file_schema) override;

    Status
    GetTableFiles(const std::string& table_id, const std::vector<size_t>& ids, TableFilesSchema& table_files) override;

    Status
    GetTableFilesBySegmentId(const std::string& table_id, const std::string& segment_id,
                             TableFilesSchema& table_files) override;

    Status
    UpdateTableIndex(const std::string& table_id, const TableIndex& index) override;

    Status
    UpdateTableFlag(const std::string& table_id, int64_t flag) override;

    Status
    UpdateTableFlushLSN(const std::string& table_id, uint64_t flush_lsn) override;

    Status
    GetTableFlushLSN(const std::string& table_id, uint64_t& flush_lsn) override;

    Status
    GetTableFilesByFlushLSN(uint64_t flush_lsn, TableFilesSchema& table_files) override;

    Status
    UpdateTableFile(TableFileSchema& file_schema) override;

    Status
    UpdateTableFilesToIndex(const std::string& table_id) override;

    Status
    UpdateTableFiles(TableFilesSchema& files) override;

    Status
    DescribeTableIndex(const std::string& table_id, TableIndex& index) override;

    Status
    DropTableIndex(const std::string& table_id) override;

    Status
    CreatePartition(const std::string& table_id, const std::string& partition_name, const std::string& tag,
                    uint64_t lsn) override;

    Status
    DropPartition(const std::string& partition_name) override;

    Status
    ShowPartitions(const std::string& table_id, std::vector<meta::TableSchema>& partition_schema_array) override;

    Status
    GetPartitionName(const std::string& table_id, const std::string& tag, std::string& partition_name) override;

    Status
    FilesToSearch(const std::string& table_id, const std::vector<size_t>& ids, TableFilesSchema& files) override;

    Status
    FilesToMerge(const std::string& table_id, TableFilesSchema& files) override;

    Status
    FilesToIndex(TableFilesSchema&) override;

    Status
    FilesByType(const std::string& table_id, const std::vector<int>& file_types,
                TableFilesSchema& table_files) override;

    Status
    Size(uint64_t& result) override;

    Status
    Archive() override;

    Status
    CleanUpShadowFiles() override;

    Status
    CleanUpFilesWithTTL(uint64_t seconds, CleanUpFilter* filter = nullptr) override;

    Status
    DropAll() override;

    Status
    Count(const std::string& table_id, uint64_t& result) override;

    Status
    SetGlobalLastLSN(uint64_t lsn) override;

    Status
    GetGlobalLastLSN(uint64_t& lsn) override;

 private:
    Status
    NextFileId(std::string& file_id);
    Status
    NextTableId(std::string& table_id);
    Status
    DiscardFiles(int64_t to_discard_size);

    void
    ValidateMetaSchema();
    Status
    Initialize();

 private:
    const DBMetaOptions options_;
    std::mutex meta_mutex_;
    std::mutex genid_mutex_;
};  // DBMetaImpl

}  // namespace meta
}  // namespace engine
}  // namespace milvus
