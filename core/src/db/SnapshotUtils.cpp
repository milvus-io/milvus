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

#include "db/SnapshotUtils.h"
#include "db/SnapshotHandlers.h"
#include "db/SnapshotVisitor.h"
#include "db/Utils.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Snapshots.h"
#include "segment/Segment.h"

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

namespace {
constexpr const char* JSON_ROW_COUNT = "row_count";
constexpr const char* JSON_ID = "id";
constexpr const char* JSON_PARTITIONS = "partitions";
constexpr const char* JSON_PARTITION_TAG = "tag";
constexpr const char* JSON_SEGMENTS = "segments";
constexpr const char* JSON_NAME = "name";
constexpr const char* JSON_FIELDS = "fields";
constexpr const char* JSON_INDEX_NAME = "index_name";
constexpr const char* JSON_DATA_SIZE = "data_size";
}  // namespace

Status
SetSnapshotIndex(const std::string& collection_name, const std::string& field_name,
                 engine::CollectionIndex& index_info) {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
    auto field = ss->GetField(field_name);
    if (field == nullptr) {
        return Status(DB_ERROR, "Invalid field name");
    }

    snapshot::OperationContext ss_context;
    if (IsVectorField(field)) {
        auto new_element = std::make_shared<snapshot::FieldElement>(
            ss->GetCollectionId(), field->GetID(), index_info.index_name_, milvus::engine::FieldElementType::FET_INDEX);
        nlohmann::json json;
        json[engine::PARAM_INDEX_METRIC_TYPE] = index_info.metric_name_;
        json[engine::PARAM_INDEX_EXTRA_PARAMS] = index_info.extra_params_;
        new_element->SetParams(json);
        ss_context.new_field_elements.push_back(new_element);

        if (index_info.index_name_ == knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR ||
            index_info.index_name_ == knowhere::IndexEnum::INDEX_HNSW_SQ8NM) {
            auto new_element = std::make_shared<snapshot::FieldElement>(
                ss->GetCollectionId(), field->GetID(), DEFAULT_INDEX_COMPRESS_NAME,
                milvus::engine::FieldElementType::FET_COMPRESS_SQ8);
            ss_context.new_field_elements.push_back(new_element);
        }
    } else {
        auto new_element = std::make_shared<snapshot::FieldElement>(
            ss->GetCollectionId(), field->GetID(), index_info.index_name_, milvus::engine::FieldElementType::FET_INDEX);
        ss_context.new_field_elements.push_back(new_element);
    }

    auto op = std::make_shared<snapshot::AddFieldElementOperation>(ss_context, ss);
    auto status = op->Push();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
GetSnapshotIndex(const std::string& collection_name, const std::string& field_name,
                 engine::CollectionIndex& index_info) {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto field = ss->GetField(field_name);
    if (field == nullptr) {
        return Status(DB_ERROR, "Invalid field name");
    }

    auto field_elements = ss->GetFieldElementsByField(field_name);
    if (IsVectorField(field)) {
        for (auto& field_element : field_elements) {
            if (field_element->GetFtype() == (int64_t)milvus::engine::FieldElementType::FET_INDEX) {
                index_info.index_name_ = field_element->GetName();
                auto json = field_element->GetParams();
                if (json.find(engine::PARAM_INDEX_METRIC_TYPE) != json.end()) {
                    index_info.metric_name_ = json[engine::PARAM_INDEX_METRIC_TYPE];
                }
                if (json.find(engine::PARAM_INDEX_EXTRA_PARAMS) != json.end()) {
                    index_info.extra_params_ = json[engine::PARAM_INDEX_EXTRA_PARAMS];
                }
                break;
            }
        }
    } else {
        for (auto& field_element : field_elements) {
            if (field_element->GetFtype() == (int64_t)milvus::engine::FieldElementType::FET_INDEX) {
                index_info.index_name_ = DEFAULT_STRUCTURED_INDEX_NAME;
            }
        }
    }

    return Status::OK();
}

Status
DeleteSnapshotIndex(const std::string& collection_name, const std::string& field_name) {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    snapshot::OperationContext context;
    std::vector<snapshot::FieldElementPtr> elements = ss->GetFieldElementsByField(field_name);
    for (auto& element : elements) {
        if (element->GetFtype() == engine::FieldElementType::FET_INDEX ||
            element->GetFtype() == engine::FieldElementType::FET_COMPRESS_SQ8) {
            context.stale_field_elements.push_back(element);
        }
    }

    auto op = std::make_shared<snapshot::DropAllIndexOperation>(context, ss);
    STATUS_CHECK(op->Push());

    return Status::OK();
}

bool
IsVectorField(const engine::snapshot::FieldPtr& field) {
    if (field == nullptr) {
        return false;
    }

    engine::FIELD_TYPE ftype = static_cast<engine::FIELD_TYPE>(field->GetFtype());
    return ftype == engine::FIELD_TYPE::VECTOR_FLOAT || ftype == engine::FIELD_TYPE::VECTOR_BINARY;
}

Status
GetSnapshotInfo(const std::string& collection_name, nlohmann::json& json_info) {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    size_t total_row_count = 0;

    std::unordered_map<snapshot::ID_TYPE, milvus::json> partitions;
    auto partition_names = ss->GetPartitionNames();
    for (auto& name : partition_names) {
        auto partition = ss->GetPartition(name);

        milvus::json json_partition;
        json_partition[JSON_PARTITION_TAG] = name;
        json_partition[JSON_ID] = partition->GetID();

        auto partition_commit = ss->GetPartitionCommitByPartitionId(partition->GetID());
        json_partition[JSON_ROW_COUNT] = partition_commit->GetRowCount();
        total_row_count += partition_commit->GetRowCount();

        partitions.insert(std::make_pair(partition->GetID(), json_partition));
    }

    snapshot::IDS_TYPE segment_ids;
    auto handler = std::make_shared<SegmentsToSearchCollector>(ss, segment_ids);
    handler->Iterate();

    std::unordered_map<snapshot::ID_TYPE, std::vector<milvus::json>> segments;
    for (auto id : segment_ids) {
        auto segment_commit = ss->GetSegmentCommitBySegmentId(id);
        if (segment_commit == nullptr) {
            continue;
        }

        milvus::json json_fields;
        auto seg_visitor = engine::SegmentVisitor::Build(ss, id);
        auto& field_visitors = seg_visitor->GetFieldVisitors();
        for (auto& iter : field_visitors) {
            milvus::json json_field;
            const engine::snapshot::FieldPtr& field = iter.second->GetField();
            json_field[JSON_NAME] = field->GetName();

            std::string index_name;
            uint64_t total_size = 0;
            auto element_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_RAW);
            if (element_visitor) {
                if (element_visitor->GetFile()) {
                    total_size += element_visitor->GetFile()->GetSize();
                }
                if (element_visitor->GetElement()) {
                    index_name = element_visitor->GetElement()->GetName();
                }
            }

            auto index_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_INDEX);
            if (index_visitor && index_visitor->GetFile()) {
                if (index_visitor->GetFile()) {
                    total_size += index_visitor->GetFile()->GetSize();
                }
                if (index_visitor->GetElement()) {
                    index_name = index_visitor->GetElement()->GetName();
                }
            }

            auto compress_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_COMPRESS_SQ8);
            if (compress_visitor && compress_visitor->GetFile()) {
                total_size += compress_visitor->GetFile()->GetSize();
            }

            json_field[JSON_INDEX_NAME] = index_name;
            json_field[JSON_DATA_SIZE] = total_size;
        }

        milvus::json json_segment;
        json_segment[JSON_ID] = id;
        json_segment[JSON_ROW_COUNT] = segment_commit->GetRowCount();
        json_segment[JSON_DATA_SIZE] = segment_commit->GetSize();
        json_segment[JSON_FIELDS] = json_fields;
        segments[segment_commit->GetPartitionId()].push_back(json_segment);
    }

    milvus::json json_partitions;
    for (auto pair : partitions) {
        milvus::json json_segments;
        auto seg_array = segments[pair.first];
        for (auto& json : seg_array) {
            json_segments.push_back(json);
        }
        pair.second[JSON_SEGMENTS] = json_segments;
        json_partitions.push_back(pair.second);
    }

    json_info[JSON_ROW_COUNT] = total_row_count;
    json_info[JSON_PARTITIONS] = json_partitions;

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
