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

auto
CollectionPrototype(const std::string& path);

class SqliteMetaImpl : public Meta {
 public:
    explicit SqliteMetaImpl(const DBMetaOptions& options);
    ~SqliteMetaImpl();

    Status
    CreateCollection(CollectionSchema& collection_schema) override;

    Status
    DescribeCollection(CollectionSchema& collection_schema) override;

    Status
    HasCollection(const std::string& collection_id, bool& has_or_not) override;

    Status
    AllCollections(std::vector<CollectionSchema>& collection_schema_array) override;

    Status
    DropCollection(const std::string& collection_id) override;

    Status
    DeleteCollectionFiles(const std::string& collection_id) override;

    Status
    CreateCollectionFile(SegmentSchema& file_schema) override;

    Status
    GetCollectionFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                       SegmentsSchema& collection_files) override;

    Status
    GetCollectionFilesBySegmentId(const std::string& segment_id, SegmentsSchema& collection_files) override;

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

    Status
    CreateHybridCollection(CollectionSchema& collection_schema, hybrid::FieldsSchema& fields_schema) override;

    Status
    DescribeHybridCollection(CollectionSchema& collection_schema, hybrid::FieldsSchema& fields_schema) override;

    Status
    CreateHybridCollectionFile(SegmentSchema& file_schema) override;

 private:
    Status
    NextFileId(std::string& file_id);
    Status
    NextCollectionId(std::string& collection_id);
    Status
    DiscardFiles(int64_t to_discard_size);

    void
    ValidateMetaSchema();
    void
    ValidateCollectionMetaSchema();
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
