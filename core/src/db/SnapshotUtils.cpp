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

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

const char* JSON_ROW_COUNT = "row_count";
const char* JSON_ID = "id";
const char* JSON_PARTITIONS = "partitions";
const char* JSON_SEGMENTS = "segments";
const char* JSON_FIELD = "field";
const char* JSON_FIELD_ELEMENT = "field_element";
const char* JSON_PARTITION_TAG = "tag";
const char* JSON_FILES = "files";
const char* JSON_INDEX_NAME = "index_name";
const char* JSON_INDEX_TYPE = "index_type";
const char* JSON_DATA_SIZE = "data_size";
const char* JSON_PATH = "path";

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
    auto index_element =
        std::make_shared<snapshot::FieldElement>(ss->GetCollectionId(), field->GetID(), index_info.index_name_,
                                                 milvus::engine::FieldElementType::FET_INDEX, index_info.index_type_);
    ss_context.new_field_elements.push_back(index_element);
    if (IsVectorField(field)) {
        milvus::json json;
        json[engine::PARAM_INDEX_METRIC_TYPE] = index_info.metric_name_;
        json[engine::PARAM_INDEX_EXTRA_PARAMS] = index_info.extra_params_;
        index_element->SetParams(json);

        if (index_info.index_name_ == knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR ||
            index_info.index_name_ == knowhere::IndexEnum::INDEX_HNSW_SQ8NM) {
            auto compress_element = std::make_shared<snapshot::FieldElement>(
                ss->GetCollectionId(), field->GetID(), DEFAULT_INDEX_COMPRESS_NAME,
                milvus::engine::FieldElementType::FET_COMPRESS_SQ8);
            ss_context.new_field_elements.push_back(compress_element);
        }
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
                index_info.index_type_ = field_element->GetTypeName();
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
                index_info.index_name_ = field_element->GetName();
                index_info.index_type_ = field_element->GetTypeName();
            }
        }
    }

    return Status::OK();
}

Status
DeleteSnapshotIndex(const std::string& collection_name, const std::string& field_name) {
    // drop for all fields or drop for one field?
    std::vector<std::string> field_names;
    if (field_name.empty()) {
        snapshot::ScopedSnapshotT ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
        field_names = ss->GetFieldNames();
    } else {
        field_names.push_back(field_name);
    }

    for (auto& name : field_names) {
        snapshot::ScopedSnapshotT ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));
        std::vector<snapshot::FieldElementPtr> elements = ss->GetFieldElementsByField(name);
        for (auto& element : elements) {
            if (element->GetFtype() == engine::FieldElementType::FET_INDEX ||
                element->GetFtype() == engine::FieldElementType::FET_COMPRESS_SQ8) {
                snapshot::OperationContext context;
                context.stale_field_elements.push_back(element);
                auto op = std::make_shared<snapshot::DropAllIndexOperation>(context, ss);
                STATUS_CHECK(op->Push());
            }
        }
    }

    return Status::OK();
}

bool
IsVectorField(const engine::snapshot::FieldPtr& field) {
    if (field == nullptr) {
        return false;
    }

    engine::DataType ftype = static_cast<engine::DataType>(field->GetFtype());
    return ftype == engine::DataType::VECTOR_FLOAT || ftype == engine::DataType::VECTOR_BINARY;
}

Status
GetSnapshotInfo(const std::string& collection_name, milvus::json& json_info) {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    size_t total_row_count = 0;
    size_t total_data_size = 0;

    // get partition information
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
        json_partition[JSON_DATA_SIZE] = partition_commit->GetSize();
        total_data_size += partition_commit->GetSize();

        partitions.insert(std::make_pair(partition->GetID(), json_partition));
    }

    // just ensure segments listed in id order
    snapshot::IDS_TYPE segment_ids;
    auto handler = std::make_shared<SegmentsToSearchCollector>(ss, segment_ids);
    handler->Iterate();
    std::sort(segment_ids.begin(), segment_ids.end());

    // get segment information and construct segment json nodes
    std::unordered_map<snapshot::ID_TYPE, std::vector<milvus::json>> json_partition_segments;
    for (auto id : segment_ids) {
        auto segment_commit = ss->GetSegmentCommitBySegmentId(id);
        if (segment_commit == nullptr) {
            continue;
        }

        milvus::json json_files;
        auto seg_visitor = engine::SegmentVisitor::Build(ss, id);
        auto& field_visitors = seg_visitor->GetFieldVisitors();
        for (auto& iter : field_visitors) {
            const engine::snapshot::FieldPtr& field = iter.second->GetField();

            auto& elements = iter.second->GetElementVistors();
            for (auto pair : elements) {
                if (pair.second == nullptr || pair.second->GetElement() == nullptr) {
                    continue;
                }

                milvus::json json_file;
                auto element = pair.second->GetElement();
                if (pair.second->GetFile()) {
                    json_file[JSON_DATA_SIZE] = pair.second->GetFile()->GetSize();
                    json_file[JSON_PATH] =
                        engine::snapshot::GetResPath<engine::snapshot::SegmentFile>("", pair.second->GetFile());
                    json_file[JSON_FIELD] = field->GetName();

                    // if the element is index, print index name/type
                    // else print element name
                    if (element->GetFtype() == engine::FieldElementType::FET_INDEX) {
                        json_file[JSON_INDEX_NAME] = element->GetName();
                        json_file[JSON_INDEX_TYPE] = element->GetTypeName();
                    } else {
                        json_file[JSON_FIELD_ELEMENT] = element->GetName();
                    }
                }
                json_files.push_back(json_file);
            }
        }

        milvus::json json_segment;
        json_segment[JSON_ID] = id;
        json_segment[JSON_ROW_COUNT] = segment_commit->GetRowCount();
        json_segment[JSON_DATA_SIZE] = segment_commit->GetSize();
        json_segment[JSON_FILES] = json_files;
        json_partition_segments[segment_commit->GetPartitionId()].push_back(json_segment);
    }

    // construct partition json nodes
    milvus::json json_partitions;
    for (auto pair : partitions) {
        milvus::json json_segments;
        auto seg_array = json_partition_segments[pair.first];
        for (auto& json : seg_array) {
            json_segments.push_back(json);
        }
        pair.second[JSON_SEGMENTS] = json_segments;
        json_partitions.push_back(pair.second);
    }

    json_info[JSON_ROW_COUNT] = total_row_count;
    json_info[JSON_DATA_SIZE] = total_data_size;
    json_info[JSON_PARTITIONS] = json_partitions;

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
