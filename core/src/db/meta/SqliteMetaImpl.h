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
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "Meta.h"
#include "db/Options.h"

#include <sqlite3.h>

namespace milvus {
namespace engine {
namespace meta {

using AttrsMap = std::unordered_map<std::string, std::string>;
using AttrsMapList = std::vector<AttrsMap>;

class SqliteMetaImpl : public Meta {
 public:
    explicit SqliteMetaImpl(const DBMetaOptions& options);
    ~SqliteMetaImpl();

    Status
    CreateCollection(CollectionSchema& collection_schema) override;

    Status
    DescribeCollection(CollectionSchema& collection_schema) override;

    Status
    HasCollection(const std::string& collection_id, bool& has_or_not, bool is_root = false) override;

    Status
    AllCollections(std::vector<CollectionSchema>& collection_schema_array, bool is_root = false) override;

    Status
    DropCollections(const std::vector<std::string>& collection_id_array) override;

    Status
    DeleteCollectionFiles(const std::vector<std::string>& collection_id_array) override;

    Status
    CreateCollectionFile(SegmentSchema& file_schema) override;

    Status
    GetCollectionFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                       FilesHolder& files_holder) override;

    Status
    GetCollectionFilesBySegmentId(const std::string& segment_id, FilesHolder& files_holder) override;

    Status
    UpdateCollectionIndex(const std::string& collection_id, const CollectionIndex& index) override;

    Status
    UpdateCollectionFlag(const std::string& collection_id, int64_t flag) override;

    Status
    UpdateCollectionFlushLSN(const std::string& collection_id, uint64_t flush_lsn) override;

    Status
    GetCollectionFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) override;

    Status
    UpdateCollectionFile(SegmentSchema& file_schema) override;

    Status
    UpdateCollectionFilesToIndex(const std::string& collection_id) override;

    Status
    UpdateCollectionFiles(SegmentsSchema& files) override;

    Status
    UpdateCollectionFilesRowCount(SegmentsSchema& files) override;

    Status
    DescribeCollectionIndex(const std::string& collection_id, CollectionIndex& index) override;

    Status
    DropCollectionIndex(const std::string& collection_id) override;

    Status
    CreatePartition(const std::string& collection_id, const std::string& partition_name, const std::string& tag,
                    uint64_t lsn) override;

    Status
    HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) override;

    Status
    DropPartition(const std::string& partition_name) override;

    Status
    ShowPartitions(const std::string& collection_id,
                   std::vector<meta::CollectionSchema>& partition_schema_array) override;

    Status
    CountPartitions(const std::string& collection_id, int64_t& partition_count) override;

    Status
    GetPartitionName(const std::string& collection_id, const std::string& tag, std::string& partition_name) override;

    Status
    FilesToSearch(const std::string& collection_id, FilesHolder& files_holder, 
                  bool is_all_search_file = true) override;

    Status
    FilesToSearchEx(const std::string& root_collection, const std::set<std::string>& partition_id_array,
                    FilesHolder& files_holder, bool is_all_search_file = true) override;

    Status
    FilesToMerge(const std::string& collection_id, FilesHolder& files_holder) override;

    Status
    FilesToIndex(FilesHolder& files_holder) override;

    Status
    FilesByType(const std::string& collection_id, const std::vector<int>& file_types,
                FilesHolder& files_holder) override;

    Status
    FilesByTypeEx(const std::vector<meta::CollectionSchema>& collections, const std::vector<int>& file_types,
                  FilesHolder& files_holder) override;

    Status
    FilesByID(const std::vector<size_t>& ids, FilesHolder& files_holder) override;

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
    NextCollectionId(std::string& collection_id);
    Status
    DiscardFiles(int64_t to_discard_size);

    void
    ValidateMetaSchema();
    Status
    Initialize();

    Status
    SqlQuery(const std::string& sql, AttrsMapList* res = nullptr);

    Status
    SqlTransaction(const std::vector<std::string>& sql_statements);

 private:
    const DBMetaOptions options_;
    std::mutex sqlite_mutex_;     // make sqlite query/execute action to be atomic
    std::mutex operation_mutex_;  // make operation such  UpdateTableFiles to be atomic
    std::mutex genid_mutex_;

    sqlite3* db_ = nullptr;
};  // DBMetaImpl

}  // namespace meta
}  // namespace engine
}  // namespace milvus
