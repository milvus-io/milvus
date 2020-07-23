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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Options.h"
#include "Types.h"
#include "context/HybridSearchContext.h"
#include "db/snapshot/Context.h"
#include "meta/Meta.h"
#include "query/GeneralQuery.h"
#include "segment/Segment.h"
#include "server/context/Context.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

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
    CreateCollection(meta::CollectionSchema& table_schema_) = 0;

    virtual Status
    DropCollection(const std::string& collection_id) = 0;

    virtual Status
    DescribeCollection(meta::CollectionSchema& table_schema_) = 0;

    virtual Status
    HasCollection(const std::string& collection_id, bool& has_or_not) = 0;

    virtual Status
    HasNativeCollection(const std::string& collection_id, bool& has_or_not) = 0;

    virtual Status
    AllCollections(std::vector<std::string>& names) = 0;

    virtual Status
    GetCollectionInfo(const std::string& collection_id, std::string& collection_info) = 0;

    virtual Status
    GetCollectionRowCount(const std::string& collection_id, uint64_t& row_count) = 0;

    virtual Status
    PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                      bool force = false) = 0;

    virtual Status
    ReLoadSegmentsDeletedDocs(const std::string& collection_id, const std::vector<int64_t>& segment_ids) = 0;

    virtual Status
    UpdateCollectionFlag(const std::string& collection_id, int64_t flag) = 0;

    virtual Status
    CreatePartition(const std::string& collection_id, const std::string& partition_name,
                    const std::string& partition_tag) = 0;

    virtual Status
    HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) = 0;

    virtual Status
    DropPartition(const std::string& partition_name) = 0;

    virtual Status
    DropPartitionByTag(const std::string& collection_id, const std::string& partition_tag) = 0;

    virtual Status
    ShowPartitions(const std::string& collection_id, std::vector<meta::CollectionSchema>& partition_schema_array) = 0;

    virtual Status
    InsertVectors(const std::string& collection_id, const std::string& partition_tag, VectorsData& vectors) = 0;

    virtual Status
    DeleteEntities(const std::string& collection_id, IDNumbers entity_ids) = 0;

    virtual Status
    Flush(const std::string& collection_id) = 0;

    virtual Status
    Flush() = 0;

    virtual Status
    Compact(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
            double threshold = 0.0) = 0;

    virtual Status
    GetVectorsByID(const engine::meta::CollectionSchema& collection, const IDNumbers& id_array,
                   std::vector<engine::VectorsData>& vectors) = 0;

    virtual Status
    GetEntitiesByID(const std::string& collection_id, const IDNumbers& id_array,
                    const std::vector<std::string>& field_names, std::vector<engine::VectorsData>& vectors,
                    std::vector<engine::AttrsData>& attrs) = 0;

    virtual Status
    GetVectorIDs(const std::string& collection_id, const std::string& segment_id, IDNumbers& vector_ids) = 0;

    //    virtual Status
    //    Merge(const std::set<std::string>& table_ids) = 0;

    virtual Status
    QueryByIDs(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
               const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
               const IDNumbers& id_array, ResultIds& result_ids, ResultDistances& result_distances) = 0;

    virtual Status
    Query(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
          const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
          VectorsData& vectors, ResultIds& result_ids, ResultDistances& result_distances) = 0;

    virtual Status
    QueryByFileID(const std::shared_ptr<server::Context>& context, const std::vector<std::string>& file_ids, uint64_t k,
                  const milvus::json& extra_params, VectorsData& vectors, ResultIds& result_ids,
                  ResultDistances& result_distances) = 0;

    virtual Status
    Size(uint64_t& result) = 0;

    virtual Status
    CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                const CollectionIndex& index) = 0;

    virtual Status
    DescribeIndex(const std::string& collection_id, CollectionIndex& index) = 0;

    virtual Status
    DropIndex(const std::string& collection_id) = 0;

    virtual Status
    DropAll() = 0;

    virtual Status
    CreateHybridCollection(meta::CollectionSchema& collection_schema, meta::hybrid::FieldsSchema& fields_schema) = 0;

    virtual Status
    DescribeHybridCollection(meta::CollectionSchema& collection_schema, meta::hybrid::FieldsSchema& fields_schema) = 0;

    virtual Status
    InsertEntities(const std::string& collection_id, const std::string& partition_tag,
                   const std::vector<std::string>& field_names, Entity& entity,
                   std::unordered_map<std::string, meta::hybrid::DataType>& field_types) = 0;

    virtual Status
    HybridQuery(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                const std::vector<std::string>& partition_tags, query::GeneralQueryPtr general_query,
                query::QueryPtr query_ptr, std::vector<std::string>& field_name,
                std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
                engine::QueryResult& result) = 0;
    virtual Status
    FlushAttrsIndex(const std::string& collection_id) = 0;

    virtual Status
    CreateStructuredIndex(const std::string& collection_id, const std::vector<std::string>& field_names,
                          const std::unordered_map<std::string, meta::hybrid::DataType>& attr_types,
                          const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data,
                          std::unordered_map<std::string, int64_t>& attr_size,
                          std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes) = 0;
};  // DB

using DBPtr = std::shared_ptr<DB>;

}  // namespace engine
}  // namespace milvus
