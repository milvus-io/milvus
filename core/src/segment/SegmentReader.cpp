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

#include "cache/CpuCacheMgr.h"
#include "codecs/Codec.h"
#include "db/SnapshotUtils.h"
#include "db/Types.h"
#include "db/Utils.h"
#include "db/snapshot/ResourceHelper.h"
#include "knowhere/index/structured_index/StructuredIndexSort.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace segment {
namespace {
template <typename T>
knowhere::IndexPtr
CreateSortedIndex(engine::BinaryDataPtr& raw_data) {
    if (raw_data == nullptr) {
        return nullptr;
    }

    auto count = raw_data->data_.size() / sizeof(T);
    auto index_ptr =
        std::make_shared<knowhere::StructuredIndexSort<T>>(count, reinterpret_cast<const T*>(raw_data->data_.data()));
    return std::static_pointer_cast<knowhere::Index>(index_ptr);
}

Status
CreateStructuredIndex(const engine::DataType field_type, engine::BinaryDataPtr& raw_data,
                      knowhere::IndexPtr& index_ptr) {
    switch (field_type) {
        case engine::DataType::INT32: {
            index_ptr = CreateSortedIndex<int32_t>(raw_data);
            break;
        }
        case engine::DataType::INT64: {
            index_ptr = CreateSortedIndex<int64_t>(raw_data);
            break;
        }
        case engine::DataType::FLOAT: {
            index_ptr = CreateSortedIndex<float>(raw_data);
            break;
        }
        case engine::DataType::DOUBLE: {
            index_ptr = CreateSortedIndex<double>(raw_data);
            break;
        }
        default: { return Status(DB_ERROR, "Field is not structured type"); }
    }
    return Status::OK();
}
}  // namespace

SegmentReader::SegmentReader(const std::string& dir_root, const engine::SegmentVisitorPtr& segment_visitor,
                             bool initialize)
    : dir_root_(dir_root), segment_visitor_(segment_visitor) {
    dir_collections_ = dir_root_ + engine::COLLECTIONS_FOLDER;
    if (initialize) {
        Initialize();
    }
}

Status
SegmentReader::Initialize() {
    std::string directory =
        engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment_visitor_->GetSegment());

    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    fs_ptr_ = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);

    segment_ptr_ = std::make_shared<engine::Segment>();

    auto& field_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        auto ftype = static_cast<engine::DataType>(field->GetFtype());
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
    LoadDeletedDocs(deleted_docs_ptr);

    STATUS_CHECK(LoadIndice());

    return Status::OK();
}

Status
SegmentReader::LoadField(const std::string& field_name, engine::BinaryDataPtr& raw, bool to_cache) {
    try {
        TimeRecorderAuto recorder("SegmentReader::LoadField: " + field_name);

        segment_ptr_->GetFixedFieldData(field_name, raw);
        if (raw != nullptr) {
            return Status::OK();  // already exist
        }

        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        if (field_visitor == nullptr) {
            return Status(DB_ERROR, "Invalid field name");
        }

        auto raw_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW);
        if (raw_visitor->GetFile() == nullptr) {
            std::string emsg = "File of field " + field_name + " is not found";
            return Status(DB_FILE_NOT_FOUND, emsg);
        }
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, raw_visitor->GetFile());

        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
        if (data_obj == nullptr) {
            auto& ss_codec = codec::Codec::instance();
            STATUS_CHECK(ss_codec.GetBlockFormat()->Read(fs_ptr_, file_path, raw));

            if (to_cache) {
                cache::CpuCacheMgr::GetInstance().InsertItem(file_path, raw);  // put into cache
            }
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
        TimeRecorderAuto recorder("SegmentReader::LoadEntities: " + field_name);

        int64_t field_width = 0;
        STATUS_CHECK(segment_ptr_->GetFixedFieldWidth(field_name, field_width));
        if (field_width <= 0) {
            return Status(DB_ERROR, "Invalid field width");
        }

        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        if (field_visitor == nullptr) {
            return Status(DB_ERROR, "Invalid field name");
        }

        // copy from cache function
        auto copy_data = [&](uint8_t* src_data, int64_t src_data_size, const std::vector<int64_t>& offsets,
                             int64_t field_width, engine::BinaryDataPtr& raw) -> Status {
            if (src_data == nullptr) {
                return Status(DB_ERROR, "src_data is null pointer");
            }
            int64_t total_bytes = offsets.size() * field_width;
            raw = std::make_shared<engine::BinaryData>();
            raw->data_.resize(total_bytes);

            // copy from cache
            int64_t target_poz = 0;
            for (auto offset : offsets) {
                int64_t src_poz = offset * field_width;
                if (offset < 0 || src_poz + field_width > src_data_size) {
                    return Status(DB_ERROR, "Invalid entity offset");
                }

                memcpy(raw->data_.data() + target_poz, src_data + src_poz, field_width);
                target_poz += field_width;
            }

            return Status::OK();
        };

        // if raw data is alrady in cache, copy from cache
        const engine::snapshot::FieldPtr& field = field_visitor->GetField();
        engine::BinaryDataPtr field_data;
        segment_ptr_->GetFixedFieldData(field_name, field_data);
        if (field_data != nullptr) {
            return copy_data(field_data->data_.data(), field_data->data_.size(), offsets, field_width, raw);
        }

        // for vector field, the LoadVectorIndex() could create a IDMAP index in cache, copy from the index
        if (engine::IsVectorField(field)) {
            std::string temp_index_path;
            GetTempIndexPath(field->GetName(), temp_index_path);

            uint8_t* src_data = nullptr;
            int64_t src_data_size = 0;
            if (auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(temp_index_path)) {
                auto index_ptr = std::static_pointer_cast<knowhere::VecIndex>(data_obj);
                if (index_ptr->index_type() == knowhere::IndexEnum::INDEX_FAISS_IDMAP) {
                    auto idmap_index = std::static_pointer_cast<knowhere::IDMAP>(index_ptr);
                    src_data = (uint8_t*)idmap_index->GetRawVectors();
                    src_data_size = idmap_index->Count() * field_width;
                } else if (index_ptr->index_type() == knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP) {
                    auto idmap_index = std::static_pointer_cast<knowhere::BinaryIDMAP>(index_ptr);
                    src_data = (uint8_t*)idmap_index->GetRawVectors();
                    src_data_size = idmap_index->Count() * field_width;
                }
            }

            if (src_data) {
                return copy_data(src_data, src_data_size, offsets, field_width, raw);
            }
        }

        // read from storage
        codec::ReadRanges ranges;
        for (auto offset : offsets) {
            ranges.push_back(codec::ReadRange(offset * field_width, field_width));
        }

        auto raw_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, raw_visitor->GetFile());
        auto& ss_codec = codec::Codec::instance();
        STATUS_CHECK(ss_codec.GetBlockFormat()->Read(fs_ptr_, file_path, ranges, raw));
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
SegmentReader::LoadUids(std::vector<engine::idx_t>& uids) {
    engine::idx_t* uids_address = nullptr;
    int64_t id_count = 0;
    STATUS_CHECK(LoadUids(&uids_address, id_count));

    TimeRecorderAuto recorder("SegmentReader::LoadUids copy uids");

    uids.clear();
    uids.resize(id_count);
    memcpy(uids.data(), uids_address, id_count * sizeof(engine::idx_t));

    return Status::OK();
}

Status
SegmentReader::LoadUids(engine::idx_t** uids_addr, int64_t& count) {
    count = 0;
    if (uids_addr == nullptr) {
        return Status(DB_ERROR, "null pointer");
    }

    *uids_addr = nullptr;
    engine::BinaryDataPtr raw;
    auto status = LoadField(engine::FIELD_UID, raw);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    if (raw == nullptr) {
        return Status(DB_ERROR, "Failed to load id field");
    }

    if (raw->data_.size() % sizeof(engine::idx_t) != 0) {
        std::string err_msg = "Failed to load uids: illegal file size";
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    // the raw is holded by segment_
    *uids_addr = reinterpret_cast<engine::idx_t*>(raw->data_.data());
    count = raw->data_.size() / sizeof(engine::idx_t);

    return Status::OK();
}

Status
SegmentReader::LoadVectorIndex(const std::string& field_name, knowhere::VecIndexPtr& index_ptr, bool flat) {
    try {
        TimeRecorder recorder("SegmentReader::LoadVectorIndex: " + field_name);

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
        int64_t row_count = GetRowCount();
//        faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = nullptr;
//        segment::DeletedDocsPtr deleted_docs_ptr;
//        LoadDeletedDocs(deleted_docs_ptr);
//        if (deleted_docs_ptr != nullptr) {
//            auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();
//            if (!deleted_docs.empty()) {
//                concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(row_count);
//                for (auto& offset : deleted_docs) {
//                    concurrent_bitset_ptr->set(offset);
//                }
//            }
//        }
        recorder.RecordSection("prepare");

        knowhere::BinarySet index_data;
        knowhere::BinaryPtr raw_data, compress_data;

        // if index not specified, or index file not created, return a temp index(IDMAP type)
        auto index_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (flat || index_visitor == nullptr || index_visitor->GetFile() == nullptr) {
            std::string temp_index_path;
            GetTempIndexPath(field_name, temp_index_path);
            auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(temp_index_path);
            if (data_obj != nullptr) {
                // if the temp index is in cache, no need to create it
                index_ptr = std::static_pointer_cast<knowhere::VecIndex>(data_obj);
                segment_ptr_->SetVectorIndex(field_name, index_ptr);
                recorder.RecordSection("get temp index from cache");
            } else {
                auto& json = field->GetParams();
                if (json.find(knowhere::meta::DIM) == json.end()) {
                    return Status(DB_ERROR, "Vector field dimension undefined");
                }
                int64_t dimension = json[knowhere::meta::DIM];
                engine::BinaryDataPtr raw;
                STATUS_CHECK(LoadField(field_name, raw, false));

                // load uids
                std::shared_ptr<std::vector<int64_t>> uids_ptr = std::make_shared<std::vector<int64_t>>();
                STATUS_CHECK(LoadUids(*uids_ptr));

                auto dataset = knowhere::GenDataset(row_count, dimension, raw->data_.data());

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
                index_ptr->SetUids(uids_ptr);
//                index_ptr->SetBlacklist(concurrent_bitset_ptr);
                segment_ptr_->SetVectorIndex(field_name, index_ptr);

                cache::CpuCacheMgr::GetInstance().InsertItem(temp_index_path, index_ptr);
                recorder.RecordSection("construct temp IDMAP index");
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

        STATUS_CHECK(ss_codec.GetVectorIndexFormat()->ReadIndex(fs_ptr_, index_file_path, index_data));
        recorder.RecordSection("read index file: " + index_file_path);

        // for some kinds index(IVF), read raw file
        auto index_type = index_visitor->GetElement()->GetTypeName();
        if (engine::utils::RequireRawFile(index_type)) {
            engine::BinaryDataPtr fixed_data;
            auto status = segment_ptr_->GetFixedFieldData(field_name, fixed_data);
            if (status.ok()) {
                STATUS_CHECK(ss_codec.GetVectorIndexFormat()->ConvertRaw(fixed_data, raw_data));
            } else if (auto visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW)) {
                auto file_path =
                    engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
                STATUS_CHECK(ss_codec.GetVectorIndexFormat()->ReadRaw(fs_ptr_, file_path, raw_data));

                recorder.RecordSection("read raw file: " + file_path);
            }
        }

        // for some kinds index(RHNSWSQ), read compress file
        if (engine::utils::RequireCompressFile(index_type)) {
            if (auto visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_COMPRESS)) {
                auto file_path =
                    engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
                STATUS_CHECK(ss_codec.GetVectorIndexFormat()->ReadCompress(fs_ptr_, file_path, compress_data));

                recorder.RecordSection("read compress file: " + file_path);
            }
        }

        STATUS_CHECK(ss_codec.GetVectorIndexFormat()->ConstructIndex(index_type, index_data, raw_data, compress_data,
                                                                     index_ptr));

        // load uids
        std::shared_ptr<std::vector<int64_t>> uids_ptr = std::make_shared<std::vector<int64_t>>();
        STATUS_CHECK(LoadUids(*uids_ptr));

        index_ptr->SetUids(uids_ptr);
//        index_ptr->SetBlacklist(concurrent_bitset_ptr);
        segment_ptr_->SetVectorIndex(field_name, index_ptr);

        cache::CpuCacheMgr::GetInstance().InsertItem(index_file_path, index_ptr);  // put into cache
        recorder.RecordSection("construct index");
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
        TimeRecorder recorder("SegmentReader::LoadStructuredIndex: " + field_name);

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

        auto index_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (index_visitor && index_visitor->GetFile() != nullptr) {
            // read field index
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, index_visitor->GetFile());

            // if the data is in cache, no need to read file
            auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
            if (data_obj == nullptr) {
                STATUS_CHECK(ss_codec.GetStructuredIndexFormat()->Read(fs_ptr_, file_path, index_ptr));
                cache::CpuCacheMgr::GetInstance().InsertItem(file_path, index_ptr);  // put into cache
                recorder.RecordSection("read from storage");
            } else {
                index_ptr = std::static_pointer_cast<knowhere::Index>(data_obj);
                recorder.RecordSection("get from cache");
            }
        } else {
            // if index not specified, or index file not created, return a temp index(SORTED type)
            std::string temp_index_path;
            GetTempIndexPath(field_name, temp_index_path);
            auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(temp_index_path);
            if (data_obj != nullptr) {
                // if the temp index is in cache, no need to create it
                index_ptr = std::static_pointer_cast<knowhere::VecIndex>(data_obj);
                recorder.RecordSection("get temp index from cache");
            } else {
                // create temp index and put into cache
                engine::DataType field_type = engine::DataType::NONE;
                STATUS_CHECK(segment_ptr_->GetFieldType(field_name, field_type));

                engine::BinaryDataPtr raw_data;
                LoadField(field_name, raw_data, false);
                STATUS_CHECK(CreateStructuredIndex(field_type, raw_data, index_ptr));

                cache::CpuCacheMgr::GetInstance().InsertItem(temp_index_path, index_ptr);  // put into cache

                recorder.RecordSection("create temp index");
            }
        }

        segment_ptr_->SetStructuredIndex(field_name, index_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentReader::LoadIndice() {
    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_visitors_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();

        auto element_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor == nullptr) {
            continue;
        }

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
        TimeRecorderAuto recorder("SegmentReader::LoadBloomFilter");

        id_bloom_filter_ptr = segment_ptr_->GetBloomFilter();
        if (id_bloom_filter_ptr != nullptr) {
            return Status::OK();  // already exist
        }

        auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::FIELD_UID);
        auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_BLOOM_FILTER);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
        if (!std::experimental::filesystem::exists(file_path + codec::IdBloomFilterFormat::FilePostfix())) {
            return Status(DB_ERROR, "File doesn't exist");  // file doesn't exist
        }

        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
        if (data_obj == nullptr) {
            auto& ss_codec = codec::Codec::instance();
            STATUS_CHECK(ss_codec.GetIdBloomFilterFormat()->Read(fs_ptr_, file_path, id_bloom_filter_ptr));
            cache::CpuCacheMgr::GetInstance().InsertItem(file_path, id_bloom_filter_ptr);  // put into cache
        } else {
            id_bloom_filter_ptr = std::static_pointer_cast<segment::IdBloomFilter>(data_obj);
        }

        if (id_bloom_filter_ptr) {
            segment_ptr_->SetBloomFilter(id_bloom_filter_ptr);
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
        TimeRecorderAuto recorder("SegmentReader::LoadDeletedDocs");

        deleted_docs_ptr = segment_ptr_->GetDeletedDocs();
        if (deleted_docs_ptr != nullptr) {
            return Status::OK();  // already exist
        }

        auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::FIELD_UID);
        auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS);
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
        if (!std::experimental::filesystem::exists(file_path + codec::DeletedDocsFormat::FilePostfix())) {
            return Status(DB_ERROR, "File doesn't exist");  // file doesn't exist
        }

        // if the data is in cache, no need to read file
        auto data_obj = cache::CpuCacheMgr::GetInstance().GetItem(file_path);
        if (data_obj == nullptr) {
            auto& ss_codec = codec::Codec::instance();
            STATUS_CHECK(ss_codec.GetDeletedDocsFormat()->Read(fs_ptr_, file_path, deleted_docs_ptr));
            deleted_docs_ptr->GenBlacklist(GetRowCount());
            cache::CpuCacheMgr::GetInstance().InsertItem(file_path, deleted_docs_ptr);  // put into cache
        } else {
            deleted_docs_ptr = std::static_pointer_cast<segment::DeletedDocs>(data_obj);
        }

        if (deleted_docs_ptr) {
            segment_ptr_->SetDeletedDocs(deleted_docs_ptr);
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
        STATUS_CHECK(ss_codec.GetDeletedDocsFormat()->ReadSize(fs_ptr_, file_path, size));
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

Status
SegmentReader::GetTempIndexPath(const std::string& field_name, std::string& path) {
    if (segment_visitor_ == nullptr) {
        return Status(DB_ERROR, "Segment visitor is null pointer");
    }

    auto segment = segment_visitor_->GetSegment();
    path = engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment);
    path += "/";
    std::string temp_index_name = field_name + ".tmp.index";
    path += temp_index_name;

    return Status::OK();
}

int64_t
SegmentReader::GetRowCount() {
    engine::BinaryDataPtr raw;
    auto status = LoadField(engine::FIELD_UID, raw);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return 0;
    }

    if (raw == nullptr) {
        LOG_ENGINE_ERROR_ << "Failed to load id field";
        return 0;
    }

    if (raw->data_.size() % sizeof(engine::idx_t) != 0) {
        std::string err_msg = "Failed to load uids: illegal file size";
        LOG_ENGINE_ERROR_ << err_msg;
        return 0;
    }

    int64_t count = raw->data_.size() / sizeof(engine::idx_t);
    return count;
}

Status
SegmentReader::ClearCache() {
    TimeRecorderAuto recorder("SegmentReader::ClearCache");

    if (segment_visitor_ == nullptr) {
        return Status::OK();
    }

    auto& field_visitors = segment_visitor_->GetFieldVisitors();
    auto segment = segment_visitor_->GetSegment();
    if (segment == nullptr) {
        return Status::OK();
    }

    // remove delete docs and bloom filter from cache
    auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::FIELD_UID);
    if (uid_field_visitor) {
        if (auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_BLOOM_FILTER)) {
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
            cache::CpuCacheMgr::GetInstance().EraseItem(file_path);
        }

        if (auto visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS)) {
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, visitor->GetFile());
            cache::CpuCacheMgr::GetInstance().EraseItem(file_path);
        }
    }

    // erase raw data and index data from cache
    for (auto& pair : field_visitors) {
        auto& field_visitor = pair.second;
        if (field_visitor == nullptr || field_visitor->GetField() == nullptr) {
            continue;
        }

        // erase raw data from cache manager
        if (auto raw_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_RAW)) {
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, raw_visitor->GetFile());
            cache::CpuCacheMgr::GetInstance().EraseItem(file_path);
        }

        // erase index data from cache manager
        ClearFieldIndexCache(field_visitor);
    }

    cache::CpuCacheMgr::GetInstance().PrintInfo();
    return Status::OK();
}

Status
SegmentReader::ClearIndexCache(const std::string& field_name) {
    TimeRecorderAuto recorder("SegmentReader::ClearIndexCache");

    if (segment_visitor_ == nullptr) {
        return Status::OK();
    }

    if (field_name.empty()) {
        auto& field_visitors = segment_visitor_->GetFieldVisitors();
        for (auto& pair : field_visitors) {
            auto& field_visitor = pair.second;
            ClearFieldIndexCache(field_visitor);
        }
    } else {
        auto field_visitor = segment_visitor_->GetFieldVisitor(field_name);
        ClearFieldIndexCache(field_visitor);
    }

    return Status::OK();
}

Status
SegmentReader::ClearFieldIndexCache(const engine::SegmentVisitor::FieldVisitorT& field_visitor) {
    if (field_visitor == nullptr || field_visitor->GetField() == nullptr) {
        return Status(DB_ERROR, "null pointer");
    }

    auto index_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
    if (index_visitor == nullptr || index_visitor->GetFile() == nullptr) {
        const engine::snapshot::FieldPtr& field = field_visitor->GetField();
        // temp index
        std::string file_path;
        GetTempIndexPath(field->GetName(), file_path);
        cache::CpuCacheMgr::GetInstance().EraseItem(file_path);
    } else {
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, index_visitor->GetFile());
        cache::CpuCacheMgr::GetInstance().EraseItem(file_path);
    }

    return Status::OK();
}

}  // namespace segment
}  // namespace milvus
