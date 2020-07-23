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

#include "config/ServerConfig.h"
#include "db/Constants.h"
#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "db/meta/MetaTypes.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/Snapshots.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "metrics/Metrics.h"
#include "segment/SegmentReader.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

SSMemSegment::SSMemSegment(int64_t collection_id, int64_t partition_id, const DBOptions& options)
    : collection_id_(collection_id), partition_id_(partition_id), options_(options) {
    current_mem_ = 0;
    CreateSegment();
    ConfigMgr::GetInstance().Attach("cache.cache_insert_data", this);
}

SSMemSegment::~SSMemSegment() {
    ConfigMgr::GetInstance().Detach("cache.cache_insert_data", this);
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

    // create segment
    snapshot::OperationContext context;
    context.prev_partition = ss->GetResource<snapshot::Partition>(partition_id_);
    operation_ = std::make_shared<snapshot::NewSegmentOperation>(context, ss);
    status = operation_->CommitNewSegment(segment_);
    if (!status.ok()) {
        std::string err_msg = "SSMemSegment::CreateSegment failed: " + status.ToString();
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
        sf_context.field_element_name = engine::DEFAULT_RAW_DATA_NAME;

        snapshot::SegmentFilePtr seg_file;
        status = operation_->CommitNewSegmentFile(sf_context, seg_file);
        if (!status.ok()) {
            std::string err_msg = "SSMemSegment::CreateSegment failed: " + status.ToString();
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
        sf_context.field_name = engine::DEFAULT_UID_NAME;
        sf_context.field_element_name = engine::DEFAULT_DELETED_DOCS_NAME;

        snapshot::SegmentFilePtr delete_doc_file, bloom_filter_file;
        status = operation_->CommitNewSegmentFile(sf_context, delete_doc_file);
        if (!status.ok()) {
            std::string err_msg = "SSMemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }

        sf_context.field_element_name = engine::DEFAULT_BLOOM_FILTER_NAME;
        status = operation_->CommitNewSegmentFile(sf_context, bloom_filter_file);
        if (!status.ok()) {
            std::string err_msg = "SSMemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    auto ctx = operation_->GetContext();
    auto visitor = SegmentVisitor::Build(ss, ctx.new_segment, ctx.new_segment_files);

    // create segment writer
    segment_writer_ptr_ = std::make_shared<segment::SSSegmentWriter>(options_.meta_.path_, visitor);

    return Status::OK();
}

Status
SSMemSegment::GetSingleEntitySize(int64_t& single_size) {
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
                if (params.find(knowhere::meta::DIM) == params.end()) {
                    std::string msg = "Vector field params must contain: dimension";
                    LOG_SERVER_ERROR_ << msg;
                    return Status(DB_ERROR, msg);
                }

                int64_t dimension = params[knowhere::meta::DIM];
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
    int64_t single_entity_mem_size = 0;
    auto status = GetSingleEntitySize(single_entity_mem_size);
    if (!status.ok()) {
        return status;
    }

    size_t mem_left = GetMemLeft();
    if (mem_left >= single_entity_mem_size) {
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
SSMemSegment::Delete(segment::doc_id_t doc_id) {
    engine::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);

    // Check wither the doc_id is present, if yes, delete it's corresponding buffer
    engine::FIXED_FIELD_DATA raw_data;
    auto status = segment_ptr->GetFixedFieldData(engine::DEFAULT_UID_NAME, raw_data);
    if (!status.ok()) {
        return Status::OK();
    }

    int64_t* uids = reinterpret_cast<int64_t*>(raw_data.data());
    int64_t row_count = segment_ptr->GetRowCount();
    for (int64_t i = 0; i < row_count; i++) {
        if (doc_id == uids[i]) {
            segment_ptr->DeleteEntity(i);
        }
    }

    return Status::OK();
}

Status
SSMemSegment::Delete(const std::vector<segment::doc_id_t>& doc_ids) {
    engine::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);

    // Check wither the doc_id is present, if yes, delete it's corresponding buffer
    std::vector<segment::doc_id_t> temp;
    temp.resize(doc_ids.size());
    memcpy(temp.data(), doc_ids.data(), doc_ids.size() * sizeof(segment::doc_id_t));

    std::sort(temp.begin(), temp.end());

    engine::FIXED_FIELD_DATA raw_data;
    auto status = segment_ptr->GetFixedFieldData(engine::DEFAULT_UID_NAME, raw_data);
    if (!status.ok()) {
        return Status::OK();
    }

    int64_t* uids = reinterpret_cast<int64_t*>(raw_data.data());
    int64_t row_count = segment_ptr->GetRowCount();
    size_t deleted = 0;
    for (int64_t i = 0; i < row_count; ++i) {
        if (std::binary_search(temp.begin(), temp.end(), uids[i])) {
            segment_ptr->DeleteEntity(i - deleted);
            ++deleted;
        }
    }

    return Status::OK();
}

int64_t
SSMemSegment::GetCurrentMem() {
    return current_mem_;
}

int64_t
SSMemSegment::GetMemLeft() {
    return (MAX_TABLE_FILE_MEM - current_mem_);
}

bool
SSMemSegment::IsFull() {
    int64_t single_entity_mem_size = 0;
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

    auto status = segment_writer_ptr_->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize segment: " << segment_->GetID();
        return status;
    }

    status = operation_->CommitRowCount(segment_writer_ptr_->RowCount());
    status = operation_->Push();
    LOG_ENGINE_DEBUG_ << "New segment " << segment_->GetID() << " serialized, lsn = " << wal_lsn;
    return status;
}

int64_t
SSMemSegment::GetSegmentId() const {
    return segment_->GetID();
}

void
SSMemSegment::ConfigUpdate(const std::string& name) {
    options_.insert_cache_immediately_ = config.cache.cache_insert_data();
}

}  // namespace engine
}  // namespace milvus
