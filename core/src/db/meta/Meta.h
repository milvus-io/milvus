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

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "MetaTypes.h"
#include "db/Options.h"
#include "db/Types.h"
#include "db/meta/FilesHolder.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace meta {

extern const char* META_ENVIRONMENT;
extern const char* META_TABLES;
extern const char* META_TABLEFILES;
extern const char* META_COLLECTIONS;
extern const char* META_FIELDS;
extern const char* META_COLLECTIONFILES;

class FilesHolder;

class Meta {
    /*
 public:
    class CleanUpFilter {
     public:
        virtual bool
        IsIgnored(const SegmentSchema& schema) = 0;
    };
*/

 public:
    virtual ~Meta() = default;

    virtual Status
    CreateCollection(CollectionSchema& table_schema) = 0;

    virtual Status
    DescribeCollection(CollectionSchema& table_schema) = 0;

    virtual Status
    HasCollection(const std::string& collection_id, bool& has_or_not, bool is_root = false) = 0;

    virtual Status
    AllCollections(std::vector<CollectionSchema>& table_schema_array, bool is_root = false) = 0;

    virtual Status
    UpdateCollectionFlag(const std::string& collection_id, int64_t flag) = 0;

    virtual Status
    UpdateCollectionFlushLSN(const std::string& collection_id, uint64_t flush_lsn) = 0;

    virtual Status
    GetCollectionFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) = 0;

    virtual Status
    DropCollections(const std::vector<std::string>& collection_id_array) = 0;

    virtual Status
    DeleteCollectionFiles(const std::vector<std::string>& collection_id_array) = 0;

    virtual Status
    CreateCollectionFile(SegmentSchema& file_schema) = 0;

    virtual Status
    GetCollectionFiles(const std::string& collection_id, const std::vector<size_t>& ids, FilesHolder& files_holder) = 0;

    virtual Status
    GetCollectionFilesBySegmentId(const std::string& segment_id, FilesHolder& files_holder) = 0;

    virtual Status
    UpdateCollectionFile(SegmentSchema& file_schema) = 0;

    virtual Status
    UpdateCollectionFiles(SegmentsSchema& files) = 0;

    virtual Status
    UpdateCollectionFilesRowCount(SegmentsSchema& files) = 0;

    virtual Status
    UpdateCollectionIndex(const std::string& collection_id, const CollectionIndex& index) = 0;

    virtual Status
    UpdateCollectionFilesToIndex(const std::string& collection_id) = 0;

    virtual Status
    DescribeCollectionIndex(const std::string& collection_id, CollectionIndex& index) = 0;

    virtual Status
    DropCollectionIndex(const std::string& collection_id) = 0;

    virtual Status
    CreatePartition(const std::string& collection_id, const std::string& partition_name, const std::string& tag,
                    uint64_t lsn) = 0;

    virtual Status
    HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) = 0;

    virtual Status
    DropPartition(const std::string& partition_name) = 0;

    virtual Status
    ShowPartitions(const std::string& collection_id, std::vector<meta::CollectionSchema>& partition_schema_array) = 0;

    virtual Status
    CountPartitions(const std::string& collection_id, int64_t& partition_count) = 0;

    virtual Status
    GetPartitionName(const std::string& collection_id, const std::string& tag, std::string& partition_name) = 0;

    virtual Status
    FilesToSearch(const std::string& collection_id, FilesHolder& files_holder, bool is_all_search_file = true) = 0;

    virtual Status
    FilesToSearchEx(const std::string& root_collection, const std::set<std::string>& partition_id_array,
                    FilesHolder& files_holder, bool is_all_search_file = true) = 0;

    virtual Status
    FilesToMerge(const std::string& collection_id, FilesHolder& files_holder) = 0;

    virtual Status
    FilesToIndex(FilesHolder& files_holder) = 0;

    virtual Status
    FilesByType(const std::string& collection_id, const std::vector<int>& file_types, FilesHolder& files_holder) = 0;

    virtual Status
    FilesByTypeEx(const std::vector<meta::CollectionSchema>& collections, const std::vector<int>& file_types,
                  FilesHolder& files_holder) = 0;

    virtual Status
    FilesByID(const std::vector<size_t>& ids, FilesHolder& files_holder) = 0;

    virtual Status
    Size(uint64_t& result) = 0;

    virtual Status
    Archive() = 0;

    virtual Status
    CleanUpShadowFiles() = 0;

    virtual Status
    CleanUpFilesWithTTL(uint64_t seconds /*, CleanUpFilter* filter = nullptr*/) = 0;

    virtual Status
    DropAll() = 0;

    virtual Status
    Count(const std::string& collection_id, uint64_t& result) = 0;

    virtual Status
    SetGlobalLastLSN(uint64_t lsn) = 0;

    virtual Status
    GetGlobalLastLSN(uint64_t& lsn) = 0;
};  // MetaData

using MetaPtr = std::shared_ptr<Meta>;

}  // namespace meta
}  // namespace engine
}  // namespace milvus
