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

#include "db/merge/MergeTask.h"
#include "db/Utils.h"
#include "metrics/Metrics.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Log.h"

#include <memory>
#include <string>

namespace milvus {
namespace engine {

MergeTask::MergeTask(const meta::MetaPtr& meta_ptr, const DBOptions& options, meta::SegmentsSchema& files)
    : meta_ptr_(meta_ptr), options_(options), files_(files) {
}

Status
MergeTask::Execute() {
    if (files_.empty()) {
        return Status::OK();
    }

    // check input
    std::string collection_id = files_.front().collection_id_;
    for (auto& file : files_) {
        if (file.collection_id_ != collection_id) {
            return Status(DB_ERROR, "Cannot merge files across collections");
        }
    }

    // step 1: create collection file
    meta::SegmentSchema collection_file;
    collection_file.collection_id_ = collection_id;
    collection_file.file_type_ = meta::SegmentSchema::NEW_MERGE;
    Status status = meta_ptr_->CreateCollectionFile(collection_file);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to create collection: " << status.ToString();
        return status;
    }

    // step 2: merge files
    meta::SegmentsSchema updated;

    std::string new_segment_dir;
    utils::GetParentPath(collection_file.location_, new_segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);

    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    std::string info = "Merge task files size info:";
    for (auto& file : files_) {
        info += std::to_string(file.file_size_);
        info += ", ";

        server::CollectMergeFilesMetrics metrics;
        std::string segment_dir_to_merge;
        utils::GetParentPath(file.location_, segment_dir_to_merge);
        segment_writer_ptr->Merge(segment_dir_to_merge, collection_file.file_id_);

        auto file_schema = file;
        file_schema.file_type_ = meta::SegmentSchema::TO_DELETE;
        updated.push_back(file_schema);
        auto size = segment_writer_ptr->Size();
        if (size >= file_schema.index_file_size_) {
            break;
        }
    }
    LOG_ENGINE_DEBUG_ << info;

    // step 3: serialize to disk
    try {
        status = segment_writer_ptr->Serialize();
    } catch (std::exception& ex) {
        std::string msg = "Serialize merged index encounter exception: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << msg;
        status = Status(DB_ERROR, msg);
    }

    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to persist merged segment: " << new_segment_dir << ". Error: " << status.message();

        // if failed to serialize merge file to disk
        // typical error: out of disk space, out of memory or permission denied
        collection_file.file_type_ = meta::SegmentSchema::TO_DELETE;
        status = meta_ptr_->UpdateCollectionFile(collection_file);
        LOG_ENGINE_DEBUG_ << "Failed to update file to index, mark file: " << collection_file.file_id_
                          << " to to_delete";

        return status;
    }

    // step 4: update collection files state
    // if index type isn't IDMAP, set file type to TO_INDEX if file size exceed index_file_size
    // else set file type to RAW, no need to build index
    if (!utils::IsRawIndexType(collection_file.engine_type_)) {
        collection_file.file_type_ = (segment_writer_ptr->Size() >= collection_file.index_file_size_)
                                         ? meta::SegmentSchema::TO_INDEX
                                         : meta::SegmentSchema::RAW;
    } else {
        collection_file.file_type_ = meta::SegmentSchema::RAW;
    }
    collection_file.file_size_ = segment_writer_ptr->Size();
    collection_file.row_count_ = segment_writer_ptr->VectorCount();
    updated.push_back(collection_file);
    status = meta_ptr_->UpdateCollectionFiles(updated);
    LOG_ENGINE_DEBUG_ << "New merged segment " << collection_file.segment_id_ << " of size "
                      << segment_writer_ptr->Size() << " bytes";

    if (options_.insert_cache_immediately_) {
        segment_writer_ptr->Cache();
    }

    return status;
}

}  // namespace engine
}  // namespace milvus
