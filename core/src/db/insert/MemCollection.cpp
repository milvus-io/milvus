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

#include "db/insert/MemCollection.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <string>

#include <fiu-local.h>

#include "config/ServerConfig.h"
#include "db/Utils.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

MemCollection::MemCollection(int64_t collection_id, const DBOptions& options)
    : collection_id_(collection_id), options_(options) {
}

Status
MemCollection::Add(int64_t partition_id, const milvus::engine::VectorSourcePtr& source) {
    while (!source->AllAdded()) {
        std::lock_guard<std::mutex> lock(mutex_);
        MemSegmentPtr current_mem_segment;
        auto pair = mem_segments_.find(partition_id);
        if (pair != mem_segments_.end()) {
            MemSegmentList& segments = pair->second;
            if (!segments.empty()) {
                current_mem_segment = segments.back();
            }
        }

        Status status;
        if (current_mem_segment == nullptr || current_mem_segment->IsFull()) {
            MemSegmentPtr new_mem_segment = std::make_shared<MemSegment>(collection_id_, partition_id, options_);
            status = new_mem_segment->Add(source);
            if (status.ok()) {
                mem_segments_[partition_id].emplace_back(new_mem_segment);
            } else {
                return status;
            }
        } else {
            status = current_mem_segment->Add(source);
        }

        if (!status.ok()) {
            std::string err_msg = "Insert failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << err_msg;
            return Status(DB_ERROR, err_msg);
        }
    }
    return Status::OK();
}

Status
MemCollection::Delete(const std::vector<segment::doc_id_t>& doc_ids) {
    // Locate which collection file the doc id lands in
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& partition_segments : mem_segments_) {
            MemSegmentList& segments = partition_segments.second;
            for (auto& segment : segments) {
                segment->Delete(doc_ids);
            }
        }
    }
    // Add the id to delete list so it can be applied to other segments on disk during the next flush
    for (auto& id : doc_ids) {
        doc_ids_to_delete_.insert(id);
    }

    return Status::OK();
}

Status
MemCollection::EraseMem(int64_t partition_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto pair = mem_segments_.find(partition_id);
    if (pair != mem_segments_.end()) {
        mem_segments_.erase(pair);
    }

    return Status::OK();
}

Status
MemCollection::Serialize(uint64_t wal_lsn) {
    TimeRecorder recorder("MemCollection::Serialize collection " + collection_id_);

    if (!doc_ids_to_delete_.empty()) {
        while (true) {
            auto status = ApplyDeletes();
            if (status.ok()) {
                break;
            } else if (status.code() == SS_STALE_ERROR) {
                LOG_ENGINE_WARNING_ << "ApplyDeletes is stale, try again";
                continue;
            } else {
                return status;
            }
        }
    }

    doc_ids_to_delete_.clear();

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& partition_segments : mem_segments_) {
        MemSegmentList& segments = partition_segments.second;
        for (auto& segment : segments) {
            auto status = segment->Serialize(wal_lsn);
            if (!status.ok()) {
                return status;
            }
            LOG_ENGINE_DEBUG_ << "Flushed segment " << segment->GetSegmentId() << " of collection " << collection_id_;
        }
    }

    mem_segments_.clear();

    recorder.RecordSection("Finished flushing");

    return Status::OK();
}

int64_t
MemCollection::GetCollectionId() const {
    return collection_id_;
}

size_t
MemCollection::GetCurrentMem() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total_mem = 0;
    for (auto& partition_segments : mem_segments_) {
        MemSegmentList& segments = partition_segments.second;
        for (auto& segment : segments) {
            total_mem += segment->GetCurrentMem();
        }
    }
    return total_mem;
}

Status
MemCollection::ApplyDeletes() {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id_));

    // TODO: check stale segment files here
    snapshot::OperationContext context;
    context.lsn = lsn_;
    auto segments_op = std::make_shared<snapshot::CompoundSegmentsOperation>(context, ss);

    auto segment_executor = [&](const snapshot::SegmentPtr& segment, snapshot::SegmentIterator* iterator) -> Status {
        auto seg_visitor = engine::SegmentVisitor::Build(ss, segment->GetID());
        segment::SegmentReaderPtr segment_reader =
            std::make_shared<segment::SegmentReader>(options_.meta_.path_, seg_visitor);
        segment::IdBloomFilterPtr pre_bloom_filter;
        STATUS_CHECK(segment_reader->LoadBloomFilter(pre_bloom_filter));

        // Step 1: Check delete_id in mem
        std::vector<segment::doc_id_t> delete_ids;
        for (auto& id : doc_ids_to_delete_) {
            if (pre_bloom_filter->Check(id)) {
                delete_ids.push_back(id);
            }
        }

        if (delete_ids.empty()) {
            return Status::OK();
        }

        // Step 2: Load previous delete_id and merge into 'delete_ids'
        segment::DeletedDocsPtr prev_del_docs;
        STATUS_CHECK(segment_reader->LoadDeletedDocs(prev_del_docs));
        std::vector<segment::offset_t> pre_del_ids;
        if (prev_del_docs) {
            pre_del_ids = prev_del_docs->GetDeletedDocs();
            if (!pre_del_ids.empty())
                delete_ids.insert(delete_ids.end(), pre_del_ids.begin(), pre_del_ids.end());
        }

        // TODO(yhz): Update blacklist in cache
        //        std::vector<knowhere::VecIndexPtr> indexes;
        //        std::vector<faiss::ConcurrentBitsetPtr> blacklists;
        std::string collection_root_path = options_.meta_.path_ + COLLECTIONS_FOLDER;

        std::sort(delete_ids.begin(), delete_ids.end());
        std::set<segment::doc_id_t> ids_to_check(delete_ids.begin(), delete_ids.end());

        // Step 3: Mark previous deleted docs file and bloom filter file stale
        auto& field_visitors_map = seg_visitor->GetFieldVisitors();
        auto uid_field_visitor = seg_visitor->GetFieldVisitor(engine::DEFAULT_UID_NAME);
        auto del_doc_visitor = uid_field_visitor->GetElementVisitor(FieldElementType::FET_DELETED_DOCS);
        auto del_docs_element = del_doc_visitor->GetElement();
        auto blm_filter_visitor = uid_field_visitor->GetElementVisitor(FieldElementType::FET_BLOOM_FILTER);
        auto blm_filter_element = blm_filter_visitor->GetElement();

        auto segment_file_executor = [&](const snapshot::SegmentFilePtr& segment_file,
                                         snapshot::SegmentFileIterator* iterator) -> Status {
            if (segment_file->GetSegmentId() != segment->GetID()) {
                return Status::OK();
            }

            if (segment_file->GetFieldElementId() == del_docs_element->GetID() ||
                segment_file->GetFieldElementId() == blm_filter_element->GetID()) {
                segments_op->AddStaleSegmentFile(segment_file);
            }
            return Status::OK();
        };

        auto segment_file_iterator = std::make_shared<snapshot::SegmentFileIterator>(ss, segment_file_executor);
        segment_file_iterator->Iterate();
        STATUS_CHECK(segment_file_iterator->GetStatus());

        // Step 4: Create new deleted docs file and bloom filter file
        snapshot::SegmentFileContext del_file_context;
        del_file_context.field_name = uid_field_visitor->GetField()->GetName();
        del_file_context.field_element_name = del_docs_element->GetName();
        del_file_context.collection_id = segment->GetCollectionId();
        del_file_context.partition_id = segment->GetPartitionId();
        del_file_context.segment_id = segment->GetID();
        snapshot::SegmentFilePtr delete_file;
        STATUS_CHECK(segments_op->CommitNewSegmentFile(del_file_context, delete_file));
        auto segment_writer = std::make_shared<segment::SegmentWriter>(options_.meta_.path_, seg_visitor);

        std::string del_docs_path = snapshot::GetResPath<snapshot::SegmentFile>(collection_root_path, delete_file);

        snapshot::SegmentFileContext bloom_file_context;
        bloom_file_context.field_name = uid_field_visitor->GetField()->GetName();
        bloom_file_context.field_element_name = blm_filter_element->GetName();
        bloom_file_context.collection_id = segment->GetCollectionId();
        bloom_file_context.partition_id = segment->GetPartitionId();
        bloom_file_context.segment_id = segment->GetID();

        engine::snapshot::SegmentFile::Ptr bloom_filter_file;
        STATUS_CHECK(segments_op->CommitNewSegmentFile(bloom_file_context, bloom_filter_file));

        std::string bloom_filter_file_path =
            snapshot::GetResPath<snapshot::SegmentFile>(collection_root_path, bloom_filter_file);

        // Step 5: Write to file
        segment::IdBloomFilterPtr bloom_filter;
        STATUS_CHECK(segment_writer->CreateBloomFilter(bloom_filter_file_path, bloom_filter));
        auto delete_docs = std::make_shared<segment::DeletedDocs>();
        std::vector<segment::doc_id_t> uids;
        STATUS_CHECK(segment_reader->LoadUids(uids));
        for (size_t i = 0; i < uids.size(); i++) {
            if (std::binary_search(ids_to_check.begin(), ids_to_check.end(), uids[i])) {
                delete_docs->AddDeletedDoc(i);
            } else {
                bloom_filter->Add(uids[i]);
            }
        }

        STATUS_CHECK(
            segments_op->CommitRowCountDelta(segment->GetID(), delete_docs->GetCount() - pre_del_ids.size(), true));

        STATUS_CHECK(segment_writer->WriteDeletedDocs(del_docs_path, delete_docs));
        STATUS_CHECK(segment_writer->WriteBloomFilter(bloom_filter_file_path, bloom_filter));

        return Status::OK();
    };

    auto segment_iterator = std::make_shared<snapshot::SegmentIterator>(ss, segment_executor);
    segment_iterator->Iterate();
    STATUS_CHECK(segment_iterator->GetStatus());

    fiu_do_on("MemCollection.ApplyDeletes.RandomSleep", {
        std::srand(std::time(nullptr));
        sleep(std::rand() % 3);
    });
    return segments_op->Push();
}

uint64_t
MemCollection::GetLSN() {
    return lsn_;
}

void
MemCollection::SetLSN(uint64_t lsn) {
    lsn_ = lsn;
}

}  // namespace engine
}  // namespace milvus
