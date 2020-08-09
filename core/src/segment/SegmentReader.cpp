// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "segment/SegmentReader.h"

#include <experimental/filesystem>
#include <memory>
#include <utility>

#include "cache/CpuCacheMgr.h"
#include "codecs/Codec.h"
#include "db/SnapshotUtils.h"
#include "db/Types.h"
#include "db/snapshot/ResourceHelper.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "utils/Log.h"

namespace milvus {
namespace segment {

SegmentReader::SegmentReader(const std::string& dir_root, const engine::SegmentVisitorPtr& segment_visitor)
    : dir_root_(dir_root), segment_visitor_(segment_visitor) {
    Initialize();
}

Status
SegmentReader::Initialize() {
    dir_collections_ = dir_root_ + engine::COLLECTIONS_FOLDER;

    std::string directory =
        engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment_visitor_->GetSegment());

    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    fs_ptr_ = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);

    segment_ptr_ = std::make_shared<engine::Segment>();

    const engine::SegmentVisitor::IdMapT& field_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::DataType ftype = static_cast<engine::DataType>(field->GetFtype());
        if (engine::IsVectorField(field)) {
            json params = field->GetParams();
            if (params.find(knowhere::meta::DIM) == params.end()) {
                std::string msg = "Vector field params must contain: dimension";
                LOG_SERVER_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }

            int64_t field_width = 0;
            int64_t dimension = params[knowhere::meta::DIM];
            if (ftype == engine::DataType::VECTOR_BINARY) {
                field_width = (dimension / 8);
            } else {
                field_width = (dimension * sizeof(float));
            }
            segment_ptr_->AddField(name, ftype, field_width);
        } else {
            segment_ptr_->AddField(name, ftype);
        }
    }

    return Status::OK();
}

Status
SegmentReader::Load() {
    STATUS_CHECK(LoadFields());

    segment::IdBloomFilterPtr id_bloom_filter_ptr;
    STATUS_CHECK(LoadBloomFilter(id_bloom_filter_ptr));

    segment::DeletedDocsPtr deleted_docs_ptr;
    STATUS_CHECK(LoadDeletedDocs(deleted_docs_ptr));

    STATUS_CHECK(LoadVectorIndice());

    return Status::OK();
}

Status
SegmentReader::LoadField(const std::string& field_name, engine::BinaryDataPtr& raw) {
    try {
        segment_ptr_->GetFixedFieldData(field_name, raw);
        if (raw != nullptr) {
            return Status::OK();  // alread exist
        }

        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        if (field_visitor == nullptr) {
            return Status(DB_ERROR, "Invalid field name");
        }

        auto raw_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, raw_visitor->GetFile());

        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
        if (data_obj == nullptr) {
            auto& ss_codec = codec::Codec::instance();
            ss_codec.GetBlockFormat()->Read(fs_ptr_, file_path, raw);

            cache::CpuCacheMgr::GetInstance().InsertItem(file_path, raw);  // put into cache
        } else {
            raw = std::static_pointer_cast<engine::BinaryData>(data_obj);
        }

        segment_ptr_->SetFixedFieldData(field_name, raw);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load raw vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadFields() {
    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_visitors_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::BinaryDataPtr raw_data;
        auto status = segment_ptr_->GetFixedFieldData(name, raw_data);

        if (!status.ok() || raw_data == nullptr) {
            STATUS_CHECK(LoadField(name, raw_data));
        }
    }

    return Status::OK();
}

Status
SegmentReader::LoadEntities(const std::string& field_name, const std::vector<int64_t>& offsets,
                            engine::BinaryDataPtr& raw) {
    try {
        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        auto raw_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, raw_visitor->GetFile());

        int64_t field_width = 0;
        STATUS_CHECK(segment_ptr_->GetFixedFieldWidth(field_name, field_width));
        if (field_width <= 0) {
            return Status(DB_ERROR, "Invalid field width");
        }

        codec::ReadRanges ranges;
        for (auto offset : offsets) {
            ranges.push_back(codec::ReadRange(offset * field_width, field_width));
        }
        auto& ss_codec = codec::Codec::instance();
        ss_codec.GetBlockFormat()->Read(fs_ptr_, file_path, ranges, raw);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load raw vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentReader::LoadFieldsEntities(const std::vector<std::string>& fields_name, const std::vector<int64_t>& offsets,
                                  engine::DataChunkPtr& data_chunk) {
    if (data_chunk == nullptr) {
        data_chunk = std::make_shared<engine::DataChunk>();
    }
    data_chunk->count_ += offsets.size();
    for (auto& name : fields_name) {
        engine::BinaryDataPtr raw_data;
        auto status = LoadEntities(name, offsets, raw_data);
        if (!status.ok() || raw_data == nullptr) {
            return status;
        }

        auto& target_data = data_chunk->fixed_fields_[name];
        if (target_data != nullptr) {
            auto chunk_size = target_data->Size();
            auto raw_data_size = raw_data->Size();
            target_data->data_.resize(chunk_size + raw_data_size);
            memcpy(target_data->data_.data() + chunk_size, raw_data->data_.data(), raw_data_size);
        } else {
            data_chunk->fixed_fields_[name] = raw_data;
        }
    }
    return Status::OK();
}

Status
SegmentReader::LoadUids(std::vector<engine::id_t>& uids) {
    engine::BinaryDataPtr raw;
    auto status = LoadField(engine::FIELD_UID, raw);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    if (raw == nullptr) {
        return Status(DB_ERROR, "Failed to load id field");
    }

    if (raw->data_.size() % sizeof(engine::id_t) != 0) {
        std::string err_msg = "Failed to load uids: illegal file size";
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    uids.clear();
    uids.resize(raw->data_.size() / sizeof(engine::id_t));
    memcpy(uids.data(), raw->data_.data(), raw->data_.size());

    return Status::OK();
}

Status
SegmentReader::LoadVectorIndex(const std::string& field_name, knowhere::VecIndexPtr& index_ptr, bool flat) {
    try {
        segment_ptr_->GetVectorIndex(field_name, index_ptr);
        if (index_ptr != nullptr) {
            return Status::OK();  // already exist
        }

        // check field type
        auto& ss_codec = codec::Codec::instance();
        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        const engine::snapshot::FieldPtr& field = field_visitor->GetField();
        if (!engine::IsVectorField(field)) {
            return Status(DB_ERROR, "Field is not vector type");
        }

        // load deleted doc
        auto& segment = segment_visitor_->GetSegment();
        auto& snapshot = segment_visitor_->GetSnapshot();
        auto segment_commit = snapshot->GetSegmentCommitBySegmentId(segment->GetID());
        faiss::ConcurrentBitsetPtr concurrent_bitset_ptr =
            std::make_shared<faiss::ConcurrentBitset>(segment_commit->GetRowCount());

        segment::DeletedDocsPtr deleted_docs_ptr;
        STATUS_CHECK(LoadDeletedDocs(deleted_docs_ptr));
        if (deleted_docs_ptr != nullptr) {
            auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();
            for (auto& offset : deleted_docs) {
                concurrent_bitset_ptr->set(offset);
            }
        }

        // load uids
        std::vector<int64_t> uids;
        STATUS_CHECK(LoadUids(uids));

        knowhere::BinarySet index_data;
        knowhere::BinaryPtr raw_data, compress_data;

        // if index not specified, or index file not created, return a temp index(IDMAP type)
        auto index_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (flat || index_visitor == nullptr || index_visitor->GetFile() == nullptr) {
            auto temp_index_path = engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment);
            temp_index_path += "/";
            std::string temp_index_name = field_name + ".idmap";
            temp_index_path += temp_index_name;

            // if the data is in cache, no need to read file
            auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(temp_index_path);
            if (data_obj != nullptr) {
                index_ptr = std::static_pointer_cast<knowhere::VecIndex>(data_obj);
                segment_ptr_->SetVectorIndex(field_name, index_ptr);
            } else {
                auto& json = field->GetParams();
                if (json.find(knowhere::meta::DIM) == json.end()) {
                    return Status(DB_ERROR, "Vector field dimension undefined");
                }
                int64_t dimension = json[knowhere::meta::DIM];
                engine::BinaryDataPtr raw;
                STATUS_CHECK(LoadField(field_name, raw));

                auto dataset = knowhere::GenDataset(segment_commit->GetRowCount(), dimension, raw->data_.data());

                // construct IDMAP index
                knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
                if (field->GetFtype() == engine::DataType::VECTOR_FLOAT) {
                    index_ptr = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IDMAP,
                                                                 knowhere::IndexMode::MODE_CPU);
                } else {
                    index_ptr = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
                                                                 knowhere::IndexMode::MODE_CPU);
                }
                milvus::json conf{{knowhere::meta::DIM, dimension}};
                index_ptr->Train(knowhere::DatasetPtr(), conf);
                index_ptr->AddWithoutIds(dataset, conf);
                index_ptr->SetUids(uids);
                index_ptr->SetBlacklist(concurrent_bitset_ptr);
                segment_ptr_->SetVectorIndex(field_name, index_ptr);
            }

            return Status::OK();
        }

        // read index file
        std::string index_file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, index_visitor->GetFile());
        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(index_file_path);
        if (data_obj != nullptr) {
            index_ptr = std::static_pointer_cast<knowhere::VecIndex>(data_obj);
            segment_ptr_->SetVectorIndex(field_name, index_ptr);

            return Status::OK();
        }

        ss_codec.GetVectorIndexFormat()->ReadIndex(fs_ptr_, index_file_path, index_data);

        // for some kinds index(IVF), read raw file
        auto index_type = index_visitor->GetElement()->GetTypeName();
        if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT || index_type == knowhere::IndexEnum::INDEX_NSG ||
            index_type == knowhere::IndexEnum::INDEX_HNSW) {
            engine::BinaryDataPtr fixed_data;
            auto status = segment_ptr_->GetFixedFieldData(field_name, fixed_data);
            if (status.ok()) {
                ss_codec.GetVectorIndexFormat()->ConvertRaw(fixed_data, raw_data);
            } else if (auto visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW)) {
                auto file_path =
                    engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
                ss_codec.GetVectorIndexFormat()->ReadRaw(fs_ptr_, file_path, raw_data);
            }
        }

        // for some kinds index(SQ8), read compress file
        if (index_type == knowhere::IndexEnum::INDEX_RHNSWSQ) {
            if (auto visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_COMPRESS_SQ8)) {
                auto file_path =
                    engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
                ss_codec.GetVectorIndexFormat()->ReadCompress(fs_ptr_, file_path, compress_data);
            }
        }

        ss_codec.GetVectorIndexFormat()->ConstructIndex(index_type, index_data, raw_data, compress_data, index_ptr);

        index_ptr->SetUids(uids);
        index_ptr->SetBlacklist(concurrent_bitset_ptr);
        segment_ptr_->SetVectorIndex(field_name, index_ptr);

        cache::CpuCacheMgr::GetInstance().InsertItem(index_file_path, index_ptr);  // put into cache
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentReader::LoadStructuredIndex(const std::string& field_name, knowhere::IndexPtr& index_ptr) {
    try {
        segment_ptr_->GetStructuredIndex(field_name, index_ptr);
        if (index_ptr != nullptr) {
            return Status::OK();  // already exist
        }

        // check field type
        auto& ss_codec = codec::Codec::instance();
        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        if (!field_visitor) {
            return Status(DB_ERROR, "Field: " + field_name + " is not exist");
        }
        const engine::snapshot::FieldPtr& field = field_visitor->GetField();
        if (engine::IsVectorField(field)) {
            return Status(DB_ERROR, "Field is not structured type");
        }

        // read field index
        auto index_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (index_visitor && index_visitor->GetFile() != nullptr) {
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, index_visitor->GetFile());

            // if the data is in cache, no need to read file
            auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
            if (data_obj == nullptr) {
                ss_codec.GetStructuredIndexFormat()->Read(fs_ptr_, file_path, index_ptr);
                cache::CpuCacheMgr::GetInstance().InsertItem(file_path, index_ptr);  // put into cache
            } else {
                index_ptr = std::static_pointer_cast<knowhere::Index>(data_obj);
            }

            segment_ptr_->SetStructuredIndex(field_name, index_ptr);
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentReader::LoadVectorIndice() {
    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_visitors_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();

        auto element_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor == nullptr) {
            continue;
        }

        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, element_visitor->GetFile());
        if (engine::IsVectorField(field)) {
            knowhere::VecIndexPtr index_ptr;
            STATUS_CHECK(LoadVectorIndex(name, index_ptr));
        } else {
            knowhere::IndexPtr index_ptr;
            STATUS_CHECK(LoadStructuredIndex(name, index_ptr));
        }
    }

    return Status::OK();
}

Status
SegmentReader::LoadBloomFilter(segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    try {
        id_bloom_filter_ptr = segment_ptr_->GetBloomFilter();
        if (id_bloom_filter_ptr != nullptr) {
            return Status::OK();  // already exist
        }

        auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::FIELD_UID);
        auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_BLOOM_FILTER);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
        if (!std::experimental::filesystem::exists(file_path + codec::IdBloomFilterFormat::FilePostfix())) {
            return Status::OK();  // file doesn't exist
        }

        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
        if (data_obj == nullptr) {
            auto& ss_codec = codec::Codec::instance();
            ss_codec.GetIdBloomFilterFormat()->Read(fs_ptr_, file_path, id_bloom_filter_ptr);
        } else {
            id_bloom_filter_ptr = std::static_pointer_cast<segment::IdBloomFilter>(data_obj);
        }

        if (id_bloom_filter_ptr) {
            segment_ptr_->SetBloomFilter(id_bloom_filter_ptr);
            // TODO: disable cache for solving bloom filter ptr problem
            // cache::CpuCacheMgr::GetInstance().InsertItem(file_path, id_bloom_filter_ptr);  // put into cache
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load bloom filter: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadDeletedDocs(segment::DeletedDocsPtr& deleted_docs_ptr) {
    try {
        deleted_docs_ptr = segment_ptr_->GetDeletedDocs();
        if (deleted_docs_ptr != nullptr) {
            return Status::OK();  // already exist
        }

        auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::FIELD_UID);
        auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
        if (!std::experimental::filesystem::exists(file_path + codec::DeletedDocsFormat::FilePostfix())) {
            return Status::OK();  // file doesn't exist
        }

        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
        if (data_obj == nullptr) {
            auto& ss_codec = codec::Codec::instance();
            ss_codec.GetDeletedDocsFormat()->Read(fs_ptr_, file_path, deleted_docs_ptr);
        } else {
            deleted_docs_ptr = std::static_pointer_cast<segment::DeletedDocs>(data_obj);
        }

        if (deleted_docs_ptr) {
            segment_ptr_->SetDeletedDocs(deleted_docs_ptr);
            cache::CpuCacheMgr::GetInstance().InsertItem(file_path, deleted_docs_ptr);  // put into cache
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::ReadDeletedDocsSize(size_t& size) {
    try {
        size = 0;
        auto deleted_docs_ptr = segment_ptr_->GetDeletedDocs();
        if (deleted_docs_ptr != nullptr) {
            size = deleted_docs_ptr->GetCount();
            return Status::OK();  // already exist
        }

        auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::FIELD_UID);
        auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
        if (!std::experimental::filesystem::exists(file_path + codec::DeletedDocsFormat::FilePostfix())) {
            return Status::OK();  // file doesn't exist
        }

        auto& ss_codec = codec::Codec::instance();
        ss_codec.GetDeletedDocsFormat()->ReadSize(fs_ptr_, file_path, size);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to read deleted docs size: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::GetSegment(engine::SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SegmentReader::GetSegmentID(int64_t& id) {
    if (segment_visitor_) {
        auto segment = segment_visitor_->GetSegment();
        if (segment) {
            id = segment->GetID();
            return Status::OK();
        }
    }

    return Status(DB_ERROR, "SegmentWriter::GetSegmentID: null pointer");
}

std::string
SegmentReader::GetSegmentPath() {
    std::string seg_path =
        engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment_visitor_->GetSegment());
    return seg_path;
}

}  // namespace segment
}  // namespace milvus
