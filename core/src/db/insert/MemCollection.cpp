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

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

#include "cache/CpuCacheMgr.h"
#include "config/ServerConfig.h"
#include "db/Utils.h"
#include "db/snapshot/Snapshots.h"
#include "knowhere/index/vector_index/VecIndex.h"
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
        for (auto partition_segments : mem_segments_) {
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
        auto status = ApplyDeletes();
        if (!status.ok()) {
            return Status(DB_ERROR, status.message());
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto partition_segments : mem_segments_) {
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

//    std::
    std::unordered_map<int64_t, std::vector<segment::doc_id_t>> delete_ids_map;
    for (auto & m : mem_segment_list_) {
        auto seg_visitor = engine::SegmentVisitor::Build(ss, m->GetSegmentId());
        segment::SegmentReaderPtr segment_reader =
            std::make_shared<segment::SegmentReader>(options_.meta_.path_, seg_visitor);
        segment::IdBloomFilterPtr bloom_filter;
        STATUS_CHECK(segment_reader->LoadBloomFilter(bloom_filter));

        segment::DeletedDocsPtr prev_del_docs;
        STATUS_CHECK(segment_reader->LoadDeletedDocs(prev_del_docs));

        for (auto & id: doc_ids_to_delete_) {
            if (bloom_filter->Check(id)) {
                delete_ids_map[m->GetSegmentId()].push_back(id);
            }
        }

        auto& pre_del_ids = prev_del_docs->GetDeletedDocs();
        auto & m_del_ids = delete_ids_map[m->GetSegmentId()];
        m_del_ids.insert(m_del_ids.end(), pre_del_ids.begin(), pre_del_ids.end());

        // TODO(yhz): Update blacklist in cache
//        std::vector<knowhere::VecIndexPtr> indexes;
//        std::vector<faiss::ConcurrentBitsetPtr> blacklists;

        auto delete_docs = std::make_shared<segment::DeletedDocs>();

        auto& ids_to_check = delete_ids_map[m->GetSegmentId()];
        std::sort(ids_to_check.begin(), ids_to_check.end());

        std::vector<segment::doc_id_t> uids;
        STATUS_CHECK(segment_reader->LoadUids(uids));
        for (size_t i = 0; i < uids.size(); i++) {
            if (!std::binary_search(ids_to_check.begin(), ids_to_check.end(), uids[i])) {
                continue;
            }

            delete_docs->AddDeletedDoc(i);

            if (bloom_filter->Check(uids[i])) {
                bloom_filter->Remove(uids[i]);
            }
//        m->Serialize(lsn_);
        }

        auto& field_visitors_map = seg_visitor->GetFieldVisitors();
        auto uid_field_visitor = seg_visitor->GetFieldVisitor(engine::DEFAULT_UID_NAME);
        auto del_doc_visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS);
        // TODO(yhz): Create a new delete doc file in snapshot and obtain a new SegmentFile Res
        auto segment_writer = std::make_shared<segment::SegmentWriter>(options_.meta_.path_, seg_visitor);

        engine::snapshot::SegmentFile::Ptr del_docs_file;
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(options_.meta_.path_, del_docs_file);
        segment_writer->WriteDeletedDocs(segment_reader->GetSegmentPath(), delete_docs);
        segment_writer->WriteDeletedDocs(file_path, delete_docs);

        engine::snapshot::SegmentFile::Ptr bloom_filter_file;
        std::string bloom_filter_file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(options_.meta_.path_, bloom_filter_file);
        segment_writer->WriteBloomFilter(bloom_filter_file_path, bloom_filter);
    }

    // Get all index that contains blacklist in cache

    // Applying deletes to other segments on disk and their corresponding cache:
    // For each segment in collection:
    //     Load its bloom filter
    //     For each id in delete list:
    //         If present, add the uid to segment's uid list
    // For each segment
    //     Get its cache if exists
    //     Load its uids file.
    //     Scan the uids, if any uid in segment's uid list exists:
    //         add its offset to deletedDoc
    //         remove the id from bloom filter
    //         set black list in cache
    //     Serialize segment's deletedDoc TODO(zhiru): append directly to previous file for now, may have duplicates
    //     Serialize bloom filter

    //    LOG_ENGINE_DEBUG_ << "Applying " << doc_ids_to_delete_.size() << " deletes in collection: " << collection_id_;
    //
    //    TimeRecorder recorder("MemCollection::ApplyDeletes for collection " + collection_id_);
    //
    //    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
    //                                meta::SegmentSchema::FILE_TYPE::BACKUP};
    //    meta::FilesHolder files_holder;
    //    auto status = meta_->FilesByType(collection_id_, file_types, files_holder);
    //    if (!status.ok()) {
    //        std::string err_msg = "Failed to apply deletes: " + status.ToString();
    //        LOG_ENGINE_ERROR_ << err_msg;
    //        return Status(DB_ERROR, err_msg);
    //    }
    //
    //    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    //    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    //
    //    // which file need to be apply delete
    //    std::unordered_map<size_t, std::vector<segment::doc_id_t>> ids_to_check_map;  // file id mapping to delete ids
    //    for (auto& file : files) {
    //        std::string segment_dir;
    //        utils::GetParentPath(file.location_, segment_dir);
    //
    //        segment::SegmentReader segment_reader(segment_dir);
    //        segment::IdBloomFilterPtr id_bloom_filter_ptr;
    //        segment_reader.LoadBloomFilter(id_bloom_filter_ptr);
    //
    //        for (auto& id : doc_ids_to_delete_) {
    //            if (id_bloom_filter_ptr->Check(id)) {
    //                ids_to_check_map[file.id_].emplace_back(id);
    //            }
    //        }
    //    }
    //
    //    // release unused files
    //    for (auto& file : files) {
    //        if (ids_to_check_map.find(file.id_) == ids_to_check_map.end()) {
    //            files_holder.UnmarkFile(file);
    //        }
    //    }
    //
    //    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    //    milvus::engine::meta::SegmentsSchema hold_files = files_holder.HoldFiles();
    //    recorder.RecordSection("Found " + std::to_string(hold_files.size()) + " segment to apply deletes");
    //
    //    meta::SegmentsSchema files_to_update;
    //    for (auto& file : hold_files) {
    //        LOG_ENGINE_DEBUG_ << "Applying deletes in segment: " << file.segment_id_;
    //
    //        TimeRecorder rec("handle segment " + file.segment_id_);
    //
    //        std::string segment_dir;
    //        utils::GetParentPath(file.location_, segment_dir);
    //        segment::SegmentReader segment_reader(segment_dir);
    //
    //        auto& segment_id = file.segment_id_;
    //        meta::FilesHolder segment_holder;
    //        status = meta_->GetCollectionFilesBySegmentId(segment_id, segment_holder);
    //        if (!status.ok()) {
    //            break;
    //        }
    //
    //        // Get all index that contains blacklist in cache
    //        std::vector<knowhere::VecIndexPtr> indexes;
    //        std::vector<faiss::ConcurrentBitsetPtr> blacklists;
    //        milvus::engine::meta::SegmentsSchema& segment_files = segment_holder.HoldFiles();
    //        for (auto& segment_file : segment_files) {
    //            auto data_obj_ptr = cache::CpuCacheMgr::GetInstance()->GetIndex(segment_file.location_);
    //            auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
    //            if (index != nullptr) {
    //                faiss::ConcurrentBitsetPtr blacklist = index->GetBlacklist();
    //                if (blacklist != nullptr) {
    //                    indexes.emplace_back(index);
    //                    blacklists.emplace_back(blacklist);
    //                }
    //            }
    //        }
    //
    //        std::vector<segment::doc_id_t> uids;
    //        status = segment_reader.LoadUids(uids);
    //        if (!status.ok()) {
    //            break;
    //        }
    //        segment::IdBloomFilterPtr id_bloom_filter_ptr;
    //        status = segment_reader.LoadBloomFilter(id_bloom_filter_ptr);
    //        if (!status.ok()) {
    //            break;
    //        }
    //
    //        auto& ids_to_check = ids_to_check_map[file.id_];
    //
    //        segment::DeletedDocsPtr deleted_docs = std::make_shared<segment::DeletedDocs>();
    //
    //        rec.RecordSection("Loading uids and deleted docs");
    //
    //        std::sort(ids_to_check.begin(), ids_to_check.end());
    //
    //        rec.RecordSection("Sorting " + std::to_string(ids_to_check.size()) + " ids");
    //
    //        size_t delete_count = 0;
    //        auto find_diff = std::chrono::duration<double>::zero();
    //        auto set_diff = std::chrono::duration<double>::zero();
    //
    //        for (size_t i = 0; i < uids.size(); ++i) {
    //            auto find_start = std::chrono::high_resolution_clock::now();
    //
    //            auto found = std::binary_search(ids_to_check.begin(), ids_to_check.end(), uids[i]);
    //
    //            auto find_end = std::chrono::high_resolution_clock::now();
    //            find_diff += (find_end - find_start);
    //
    //            if (found) {
    //                auto set_start = std::chrono::high_resolution_clock::now();
    //
    //                delete_count++;
    //
    //                deleted_docs->AddDeletedDoc(i);
    //
    //                if (id_bloom_filter_ptr->Check(uids[i])) {
    //                    id_bloom_filter_ptr->Remove(uids[i]);
    //                }
    //
    //                for (auto& blacklist : blacklists) {
    //                    if (!blacklist->test(i)) {
    //                        blacklist->set(i);
    //                    }
    //                }
    //
    //                auto set_end = std::chrono::high_resolution_clock::now();
    //                set_diff += (set_end - set_start);
    //            }
    //        }
    //
    //        LOG_ENGINE_DEBUG_ << "Finding " << ids_to_check.size() << " uids in " << uids.size() << " uids took "
    //                          << find_diff.count() << " s in total";
    //        LOG_ENGINE_DEBUG_ << "Setting deleted docs and bloom filter took " << set_diff.count() << " s in total";
    //
    //        rec.RecordSection("Find uids and set deleted docs and bloom filter");
    //
    //        for (size_t i = 0; i < indexes.size(); ++i) {
    //            indexes[i]->SetBlacklist(blacklists[i]);
    //        }
    //
    //        segment::Segment tmp_segment;
    //        segment::SegmentWriter segment_writer(segment_dir);
    //        status = segment_writer.WriteDeletedDocs(deleted_docs);
    //        if (!status.ok()) {
    //            break;
    //        }
    //
    //        rec.RecordSection("Appended " + std::to_string(deleted_docs->GetSize()) + " offsets to deleted docs");
    //
    //        status = segment_writer.WriteBloomFilter(id_bloom_filter_ptr);
    //        if (!status.ok()) {
    //            break;
    //        }
    //
    //        rec.RecordSection("Updated bloom filter");
    //
    //        // Update collection file row count
    //        for (auto& segment_file : segment_files) {
    //            if (segment_file.file_type_ == meta::SegmentSchema::RAW ||
    //                segment_file.file_type_ == meta::SegmentSchema::TO_INDEX ||
    //                segment_file.file_type_ == meta::SegmentSchema::INDEX ||
    //                segment_file.file_type_ == meta::SegmentSchema::BACKUP) {
    //                segment_file.row_count_ -= delete_count;
    //                files_to_update.emplace_back(segment_file);
    //            }
    //        }
    //        rec.RecordSection("Update collection file row count in vector");
    //    }
    //
    //    recorder.RecordSection("Finished " + std::to_string(ids_to_check_map.size()) + " segment to apply deletes");
    //
    //    status = meta_->UpdateCollectionFilesRowCount(files_to_update);
    //
    //    if (!status.ok()) {
    //        std::string err_msg = "Failed to apply deletes: " + status.ToString();
    //        LOG_ENGINE_ERROR_ << err_msg;
    //        return Status(DB_ERROR, err_msg);
    //    }
    //
    //    doc_ids_to_delete_.clear();
    //
    //    recorder.RecordSection("Update deletes to meta");
    //    recorder.ElapseFromBegin("Finished deletes");

    return Status::OK();
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
