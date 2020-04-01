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
    CreateTable(CollectionSchema& table_schema) override;

    Status
    DescribeTable(CollectionSchema& table_schema) override;

    Status
    HasTable(const std::string& collection_id, bool& has_or_not) override;

    Status
    AllTables(std::vector<CollectionSchema>& table_schema_array) override;

    Status
    DropTable(const std::string& collection_id) override;

    Status
    DeleteTableFiles(const std::string& collection_id) override;

    Status
    CreateTableFile(SegmentSchema& file_schema) override;

    Status
    GetTableFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                  SegmentsSchema& table_files) override;

    Status
    GetTableFilesBySegmentId(const std::string& segment_id, SegmentsSchema& table_files) override;

    Status
    UpdateTableIndex(const std::string& collection_id, const TableIndex& index) override;

    Status
    UpdateTableFlag(const std::string& collection_id, int64_t flag) override;

    Status
    UpdateTableFlushLSN(const std::string& collection_id, uint64_t flush_lsn) override;

    Status
    GetTableFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) override;

    Status
    GetTableFilesByFlushLSN(uint64_t flush_lsn, SegmentsSchema& table_files) override;

    Status
    UpdateTableFile(SegmentSchema& file_schema) override;

    Status
    UpdateTableFilesToIndex(const std::string& collection_id) override;

    Status
    UpdateTableFiles(SegmentsSchema& files) override;

    Status
    UpdateTableFilesRowCount(SegmentsSchema& files) override;

    Status
    DescribeTableIndex(const std::string& collection_id, TableIndex& index) override;

    Status
    DropTableIndex(const std::string& collection_id) override;

    Status
    CreatePartition(const std::string& collection_id, const std::string& partition_name, const std::string& tag,
                    uint64_t lsn) override;

    Status
    DropPartition(const std::string& partition_name) override;

    Status
    ShowPartitions(const std::string& collection_id,
                   std::vector<meta::CollectionSchema>& partition_schema_array) override;

    Status
    GetPartitionName(const std::string& collection_id, const std::string& tag, std::string& partition_name) override;

    Status
    FilesToSearch(const std::string& collection_id, SegmentsSchema& files) override;

    Status
    FilesToMerge(const std::string& collection_id, SegmentsSchema& files) override;

    Status
    FilesToIndex(SegmentsSchema&) override;

    Status
    FilesByType(const std::string& collection_id, const std::vector<int>& file_types, SegmentsSchema& files) override;

    Status
    FilesByID(const std::vector<size_t>& ids, SegmentsSchema& files) override;

    Status
    Size(uint64_t& result) override;

    Status
    Archive() override;

    Status
    CleanUpShadowFiles() override;

    Status
    CleanUpFilesWithTTL(uint64_t seconds /*, CleanUpFilter* filter = nullptr*/) override;

    Status
    DropAll() override;

    Status
    Count(const std::string& collection_id, uint64_t& result) override;

    Status
    SetGlobalLastLSN(uint64_t lsn) override;

    Status
    GetGlobalLastLSN(uint64_t& lsn) override;

 private:
    Status
    NextFileId(std::string& file_id);
    Status
    NextTableId(std::string& collection_id);
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
