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
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Snapshots.h"
#include "segment/Segment.h"

#include <memory>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

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
    auto ftype = field->GetFtype();
    if (ftype == engine::FIELD_TYPE::VECTOR || ftype == engine::FIELD_TYPE::VECTOR_FLOAT ||
        ftype == engine::FIELD_TYPE::VECTOR_BINARY) {
        std::string index_name = knowhere::OldIndexTypeToStr(index_info.engine_type_);
        auto new_element = std::make_shared<snapshot::FieldElement>(ss->GetCollectionId(), field->GetID(), index_name,
                                                                    milvus::engine::FieldElementType::FET_INDEX);
        nlohmann::json json;
        json[engine::PARAM_INDEX_METRIC_TYPE] = index_info.metric_type_;
        json[engine::PARAM_INDEX_EXTRA_PARAMS] = index_info.extra_params_;
        new_element->SetParams(json);
        ss_context.new_field_elements.push_back(new_element);
    } else {
        auto new_element = std::make_shared<snapshot::FieldElement>(
            ss->GetCollectionId(), field->GetID(), "structured_index", milvus::engine::FieldElementType::FET_INDEX);
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
    index_info.engine_type_ = 0;
    index_info.metric_type_ = 0;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto field = ss->GetField(field_name);
    if (field == nullptr) {
        return Status(DB_ERROR, "Invalid field name");
    }

    auto field_elements = ss->GetFieldElementsByField(field_name);
    auto ftype = field->GetFtype();
    if (ftype == engine::FIELD_TYPE::VECTOR || ftype == engine::FIELD_TYPE::VECTOR_FLOAT ||
        ftype == engine::FIELD_TYPE::VECTOR_BINARY) {
        for (auto& field_element : field_elements) {
            if (field_element->GetFtype() == (int64_t)milvus::engine::FieldElementType::FET_INDEX) {
                std::string index_name = field_element->GetName();
                index_info.engine_type_ = knowhere::StrToOldIndexType(index_name);
                auto json = field_element->GetParams();
                if (json.find(engine::PARAM_INDEX_METRIC_TYPE) != json.end()) {
                    index_info.metric_type_ = json[engine::PARAM_INDEX_METRIC_TYPE];
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
                index_info.engine_type_ = (int32_t)engine::StructuredIndexType::SORTED;
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

}  // namespace engine
}  // namespace milvus
