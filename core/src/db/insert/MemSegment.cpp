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

#include "db/insert/MemSegment.h"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <string>
#include <vector>

#include "config/ServerConfig.h"
#include "db/Types.h"
#include "db/Utils.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/Snapshots.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "metrics/Metrics.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

MemSegment::MemSegment(int64_t collection_id, int64_t partition_id, const DBOptions& options)
    : collection_id_(collection_id), partition_id_(partition_id), options_(options) {
    current_mem_ = 0;
    //    CreateSegment();
}

Status
MemSegment::CreateSegment() {
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id_);
    if (!status.ok()) {
        std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    // create segment
    snapshot::OperationContext context;
    context.prev_partition = ss->GetResource<snapshot::Partition>(partition_id_);
    operation_ = std::make_shared<snapshot::NewSegmentOperation>(context, ss);
    status = operation_->CommitNewSegment(segment_);
    if (!status.ok()) {
        std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    // create segment raw files (placeholder)
    auto names = ss->GetFieldNames();
    for (auto& name : names) {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = collection_id_;
        sf_context.partition_id = partition_id_;
        sf_context.segment_id = segment_->GetID();
        sf_context.field_name = name;
        sf_context.field_element_name = engine::ELEMENT_RAW_DATA;

        snapshot::SegmentFilePtr seg_file;
        status = operation_->CommitNewSegmentFile(sf_context, seg_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    // create deleted_doc and bloom_filter files (placeholder)
    {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = collection_id_;
        sf_context.partition_id = partition_id_;
        sf_context.segment_id = segment_->GetID();
        sf_context.field_name = engine::FIELD_UID;
        sf_context.field_element_name = engine::ELEMENT_DELETED_DOCS;

        snapshot::SegmentFilePtr delete_doc_file, bloom_filter_file;
        status = operation_->CommitNewSegmentFile(sf_context, delete_doc_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }

        sf_context.field_element_name = engine::ELEMENT_BLOOM_FILTER;
        status = operation_->CommitNewSegmentFile(sf_context, bloom_filter_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    auto ctx = operation_->GetContext();
    auto visitor = SegmentVisitor::Build(ss, ctx.new_segment, ctx.new_segment_files);

    // create segment writer
    segment_writer_ptr_ = std::make_shared<segment::SegmentWriter>(options_.meta_.path_, visitor);

    return Status::OK();
}

Status
MemSegment::GetSingleEntitySize(int64_t& single_size) {
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id_);
    if (!status.ok()) {
        std::string err_msg = "MemSegment::SingleEntitySize failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    single_size = 0;
    std::vector<std::string> field_names = ss->GetFieldNames();
    for (auto& name : field_names) {
        snapshot::FieldPtr field = ss->GetField(name);
        DataType ftype = static_cast<DataType>(field->GetFtype());
        switch (ftype) {
            case DataType::BOOL:
                single_size += sizeof(bool);
                break;
            case DataType::DOUBLE:
                single_size += sizeof(double);
                break;
            case DataType::FLOAT:
                single_size += sizeof(float);
                break;
            case DataType::INT8:
                single_size += sizeof(uint8_t);
                break;
            case DataType::INT16:
                single_size += sizeof(uint16_t);
                break;
            case DataType::INT32:
                single_size += sizeof(uint32_t);
                break;
            case DataType::INT64:
                single_size += sizeof(uint64_t);
                break;
            case DataType::VECTOR_FLOAT:
            case DataType::VECTOR_BINARY: {
                json params = field->GetParams();
                if (params.find(knowhere::meta::DIM) == params.end()) {
                    std::string msg = "Vector field params must contain: dimension";
                    LOG_SERVER_ERROR_ << msg;
                    return Status(DB_ERROR, msg);
                }

                int64_t dimension = params[knowhere::meta::DIM];
                if (ftype == DataType::VECTOR_BINARY) {
                    single_size += (dimension / 8);
                } else {
                    single_size += (dimension * sizeof(float));
                }

                break;
            }
            default:
                break;
        }
    }

    return Status::OK();
}

Status
MemSegment::Add(const VectorSourcePtr& source) {
    int64_t single_entity_mem_size = 0;
    auto status = GetSingleEntitySize(single_entity_mem_size);
    if (!status.ok()) {
        return status;
    }

    size_t mem_left = GetMemLeft();
    if (mem_left >= single_entity_mem_size && single_entity_mem_size != 0) {
        int64_t num_entities_to_add = std::ceil(mem_left / single_entity_mem_size);
        int64_t num_entities_added;

        auto status = source->Add(segment_writer_ptr_, num_entities_to_add, num_entities_added);

        if (status.ok()) {
            current_mem_ += (num_entities_added * single_entity_mem_size);
        }
        return status;
    }
    return Status::OK();
}

Status
MemSegment::Delete(const std::vector<idx_t>& ids) {
    engine::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);

    // Check wither the doc_id is present, if yes, delete it's corresponding buffer
    std::vector<idx_t> uids;
    segment_writer_ptr_->LoadUids(uids);

    std::vector<offset_t> offsets;
    for (auto id : ids) {
        auto found = std::find(uids.begin(), uids.end(), id);
        if (found == uids.end()) {
            continue;
        }

        auto offset = std::distance(uids.begin(), found);
        offsets.push_back(offset);
    }
    segment_ptr->DeleteEntity(offsets);

    return Status::OK();
}

int64_t
MemSegment::GetCurrentMem() {
    return current_mem_;
}

int64_t
MemSegment::GetMemLeft() {
    return (MAX_TABLE_FILE_MEM - current_mem_);
}

bool
MemSegment::IsFull() {
    int64_t single_entity_mem_size = 0;
    auto status = GetSingleEntitySize(single_entity_mem_size);
    if (!status.ok()) {
        return true;
    }

    return (GetMemLeft() < single_entity_mem_size);
}

Status
MemSegment::Serialize(uint64_t wal_lsn) {
    int64_t size = GetCurrentMem();
    server::CollectSerializeMetrics metrics(size);

    // delete action could delete all entities of the segment
    // no need to serialize empty segment
    if (segment_writer_ptr_->RowCount() == 0) {
        return Status::OK();
    }

    auto status = segment_writer_ptr_->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize segment: " << segment_->GetID();
        return status;
    }

    STATUS_CHECK(operation_->CommitRowCount(segment_writer_ptr_->RowCount()));
    STATUS_CHECK(operation_->Push());
    LOG_ENGINE_DEBUG_ << "New segment " << segment_->GetID() << " serialized, lsn = " << wal_lsn;
    return Status::OK();
}

int64_t
MemSegment::GetSegmentId() const {
    return segment_->GetID();
}

}  // namespace engine
}  // namespace milvus
