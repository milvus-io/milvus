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

#include "db/insert/SSMemSegment.h"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <string>
#include <vector>

#include "db/Constants.h"
#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "db/meta/MetaTypes.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/Snapshots.h"
#include "metrics/Metrics.h"
#include "segment/SegmentReader.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

SSMemSegment::SSMemSegment(int64_t collection_id, int64_t partition_id, const DBOptions& options)
    : collection_id_(collection_id), partition_id_(partition_id), options_(options) {
    current_mem_ = 0;
    auto status = CreateSegment();
    if (status.ok()) {
        std::string directory;
        utils::CreatePath(segment_.get(), options_, directory);
        segment_writer_ptr_ = std::make_shared<segment::SegmentWriter>(directory);
    }

    SetIdentity("SSMemSegment");
    AddCacheInsertDataListener();
}

Status
SSMemSegment::CreateSegment() {
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id_);
    if (!status.ok()) {
        std::string err_msg = "SSMemSegment::CreateSegment failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    snapshot::OperationContext context;
    context.prev_partition = ss->GetResource<snapshot::Partition>(partition_id_);
    operation_ = std::make_shared<snapshot::NewSegmentOperation>(context, ss);
    status = operation_->CommitNewSegment(segment_);
    if (!status.ok()) {
        std::string err_msg = "SSMemSegment::CreateSegment failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    return status;
}

Status
SSMemSegment::GetSingleEntitySize(size_t& single_size) {
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id_);
    if (!status.ok()) {
        std::string err_msg = "SSMemSegment::SingleEntitySize failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    single_size = 0;
    std::vector<std::string> field_names = ss->GetFieldNames();
    for (auto& name : field_names) {
        snapshot::FieldPtr field = ss->GetField(name);
        meta::hybrid::DataType ftype = static_cast<meta::hybrid::DataType>(field->GetFtype());
        switch (ftype) {
            case meta::hybrid::DataType::BOOL:
                single_size += sizeof(bool);
                break;
            case meta::hybrid::DataType::DOUBLE:
                single_size += sizeof(double);
                break;
            case meta::hybrid::DataType::FLOAT:
                single_size += sizeof(float);
                break;
            case meta::hybrid::DataType::INT8:
                single_size += sizeof(uint8_t);
                break;
            case meta::hybrid::DataType::INT16:
                single_size += sizeof(uint16_t);
                break;
            case meta::hybrid::DataType::INT32:
                single_size += sizeof(uint32_t);
                break;
            case meta::hybrid::DataType::UID:
            case meta::hybrid::DataType::INT64:
                single_size += sizeof(uint64_t);
                break;
            case meta::hybrid::DataType::VECTOR:
            case meta::hybrid::DataType::VECTOR_FLOAT:
            case meta::hybrid::DataType::VECTOR_BINARY: {
                json params = field->GetParams();
                if (params.find(VECTOR_DIMENSION_PARAM) == params.end()) {
                    std::string msg = "Vector field params must contain: dimension";
                    LOG_SERVER_ERROR_ << msg;
                    return Status(DB_ERROR, msg);
                }

                int64_t dimension = params[VECTOR_DIMENSION_PARAM];
                if (ftype == meta::hybrid::DataType::VECTOR_BINARY) {
                    single_size += (dimension / 8);
                } else {
                    single_size += (dimension * sizeof(float));
                }

                break;
            }
        }
    }

    return Status::OK();
}

Status
SSMemSegment::Add(const SSVectorSourcePtr& source) {
    size_t single_entity_mem_size = 0;
    auto status = GetSingleEntitySize(single_entity_mem_size);
    if (!status.ok()) {
        return status;
    }

    size_t mem_left = GetMemLeft();
    if (mem_left >= single_entity_mem_size) {
        size_t num_entities_to_add = std::ceil(mem_left / single_entity_mem_size);
        size_t num_entities_added;

        auto status = source->Add(segment_writer_ptr_, num_entities_to_add, num_entities_added);

        if (status.ok()) {
            current_mem_ += (num_entities_added * single_entity_mem_size);
        }
        return status;
    }
    return Status::OK();
}

Status
SSMemSegment::Delete(segment::doc_id_t doc_id) {
    segment::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);
    // Check wither the doc_id is present, if yes, delete it's corresponding buffer
    auto uids = segment_ptr->vectors_ptr_->GetUids();
    auto found = std::find(uids.begin(), uids.end(), doc_id);
    if (found != uids.end()) {
        auto offset = std::distance(uids.begin(), found);
        segment_ptr->vectors_ptr_->Erase(offset);
    }

    return Status::OK();
}

Status
SSMemSegment::Delete(const std::vector<segment::doc_id_t>& doc_ids) {
    segment::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);

    // Check wither the doc_id is present, if yes, delete it's corresponding buffer

    std::vector<segment::doc_id_t> temp;
    temp.resize(doc_ids.size());
    memcpy(temp.data(), doc_ids.data(), doc_ids.size() * sizeof(segment::doc_id_t));

    std::sort(temp.begin(), temp.end());

    auto uids = segment_ptr->vectors_ptr_->GetUids();

    size_t deleted = 0;
    size_t loop = uids.size();
    for (size_t i = 0; i < loop; ++i) {
        if (std::binary_search(temp.begin(), temp.end(), uids[i])) {
            segment_ptr->vectors_ptr_->Erase(i - deleted);
            ++deleted;
        }
    }
    /*
    for (auto& doc_id : doc_ids) {
        auto found = std::find(uids.begin(), uids.end(), doc_id);
        if (found != uids.end()) {
            auto offset = std::distance(uids.begin(), found);
            segment_ptr->vectors_ptr_->Erase(offset);
            uids = segment_ptr->vectors_ptr_->GetUids();
        }
    }
    */

    return Status::OK();
}

size_t
SSMemSegment::GetCurrentMem() {
    return current_mem_;
}

size_t
SSMemSegment::GetMemLeft() {
    return (MAX_TABLE_FILE_MEM - current_mem_);
}

bool
SSMemSegment::IsFull() {
    size_t single_entity_mem_size = 0;
    auto status = GetSingleEntitySize(single_entity_mem_size);
    if (!status.ok()) {
        return true;
    }

    return (GetMemLeft() < single_entity_mem_size);
}

Status
SSMemSegment::Serialize(uint64_t wal_lsn) {
    int64_t size = GetCurrentMem();
    server::CollectSerializeMetrics metrics(size);

    snapshot::SegmentFileContext sf_context;
    sf_context.field_name = "vector";
    sf_context.field_element_name = "raw";
    sf_context.collection_id = segment_->GetCollectionId();
    sf_context.partition_id = segment_->GetPartitionId();
    sf_context.segment_id = segment_->GetID();
    snapshot::SegmentFilePtr seg_file;
    auto status = operation_->CommitNewSegmentFile(sf_context, seg_file);

    segment_writer_ptr_->SetSegmentName(std::to_string(segment_->GetID()));
    status = segment_writer_ptr_->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize segment: " << segment_->GetID();
        return status;
    }

    seg_file->SetSize(segment_writer_ptr_->Size());
    seg_file->SetRowCount(segment_writer_ptr_->VectorCount());

    status = operation_->Push();

    LOG_ENGINE_DEBUG_ << "New file " << seg_file->GetID() << " of size " << seg_file->GetSize()
                      << " bytes, lsn = " << wal_lsn;

    return status;
}

int64_t
SSMemSegment::GetSegmentId() const {
    return segment_->GetID();
}

void
SSMemSegment::OnCacheInsertDataChanged(bool value) {
    options_.insert_cache_immediately_ = value;
}

}  // namespace engine
}  // namespace milvus
