// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/type_c.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "cachinglayer/lrucache/DList.h"
#include "index/Index.h"
namespace milvus {

using namespace cachinglayer;

class TestChunkTranslator : public Translator<milvus::Chunk> {
 public:
    TestChunkTranslator(std::vector<int64_t> num_rows_per_chunk,
                        std::string key,
                        std::vector<std::unique_ptr<Chunk>>&& chunks)
        : Translator<milvus::Chunk>(),
          num_cells_(num_rows_per_chunk.size()),
          chunks_(std::move(chunks)),
          meta_(segcore::storagev1translator::CTMeta(
              StorageType::MEMORY,
              CellIdMappingMode::IDENTICAL,
              CellDataType::SCALAR_FIELD,
              CacheWarmupPolicy::CacheWarmupPolicy_Disable,
              true)) {
        meta_.num_rows_until_chunk_.reserve(num_cells_ + 1);
        meta_.num_rows_until_chunk_.push_back(0);
        int total_rows = 0;
        for (int i = 0; i < num_cells_; ++i) {
            meta_.num_rows_until_chunk_.push_back(
                meta_.num_rows_until_chunk_[i] + num_rows_per_chunk[i]);
            total_rows += num_rows_per_chunk[i];
        }
        key_ = key;
        segcore::storagev1translator::virtual_chunk_config(
            total_rows,
            num_cells_,
            meta_.num_rows_until_chunk_,
            meta_.virt_chunk_order_,
            meta_.vcid_to_cid_arr_);
    }
    ~TestChunkTranslator() override {
    }

    size_t
    num_cells() const override {
        return num_cells_;
    }

    cid_t
    cell_id_of(uid_t uid) const override {
        return uid;
    }

    std::pair<ResourceUsage, ResourceUsage>
    estimated_byte_size_of_cell(cid_t cid) const override {
        return {{0, 0}, {0, 0}};
    }

    int64_t
    cells_storage_bytes(const std::vector<cid_t>& cids) const override {
        return 0;
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::Chunk>>>
    get_cells(const std::vector<cid_t>& cids) override {
        std::vector<std::pair<cid_t, std::unique_ptr<milvus::Chunk>>> res;
        res.reserve(cids.size());
        for (auto cid : cids) {
            AssertInfo(cid < chunks_.size() && chunks_[cid] != nullptr,
                       "TestChunkTranslator assumes no eviction.");
            res.emplace_back(cid, std::move(chunks_[cid]));
        }
        return res;
    }

 private:
    size_t num_cells_;
    segcore::storagev1translator::CTMeta meta_;
    std::string key_;
    std::vector<std::unique_ptr<Chunk>> chunks_;
};

class TestGroupChunkTranslator : public Translator<milvus::GroupChunk> {
 public:
    TestGroupChunkTranslator(size_t num_fields,
                             std::vector<int64_t> num_rows_per_chunk,
                             std::string key,
                             std::vector<std::unique_ptr<GroupChunk>>&& chunks)
        : Translator<milvus::GroupChunk>(),
          num_cells_(num_rows_per_chunk.size()),
          chunks_(std::move(chunks)),
          meta_(segcore::storagev2translator::GroupCTMeta(
              num_fields,
              StorageType::MEMORY,
              CellIdMappingMode::IDENTICAL,
              CellDataType::OTHER,
              CacheWarmupPolicy::CacheWarmupPolicy_Disable,
              true)) {
        meta_.num_rows_until_chunk_.reserve(num_cells_ + 1);
        meta_.num_rows_until_chunk_.push_back(0);
        for (int i = 0; i < num_cells_; ++i) {
            meta_.num_rows_until_chunk_.push_back(
                meta_.num_rows_until_chunk_[i] + num_rows_per_chunk[i]);
        }
        key_ = key;
    }
    ~TestGroupChunkTranslator() override {
    }

    size_t
    num_cells() const override {
        return num_cells_;
    }

    cid_t
    cell_id_of(uid_t uid) const override {
        return uid;
    }

    std::pair<ResourceUsage, ResourceUsage>
    estimated_byte_size_of_cell(cid_t cid) const override {
        return {{0, 0}, {0, 0}};
    }

    int64_t
    cells_storage_bytes(const std::vector<cid_t>& cids) const override {
        return 0;
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::GroupChunk>>>
    get_cells(const std::vector<cid_t>& cids) override {
        std::vector<std::pair<cid_t, std::unique_ptr<milvus::GroupChunk>>> res;
        res.reserve(cids.size());
        for (auto cid : cids) {
            AssertInfo(cid < chunks_.size() && chunks_[cid] != nullptr,
                       "TestGroupChunkTranslator assumes no eviction.");
            res.emplace_back(cid, std::move(chunks_[cid]));
        }
        return res;
    }

 private:
    size_t num_cells_;
    std::vector<std::unique_ptr<GroupChunk>> chunks_;
    std::string key_;
    segcore::storagev2translator::GroupCTMeta meta_;
};

class TestIndexTranslator : public Translator<milvus::index::IndexBase> {
 public:
    TestIndexTranslator(std::string key,
                        std::unique_ptr<milvus::index::IndexBase>&& index)
        : Translator<milvus::index::IndexBase>(),
          key_(key),
          index_(std::move(index)),
          meta_(milvus::cachinglayer::Meta(
              StorageType::MEMORY,
              CellIdMappingMode::IDENTICAL,
              CellDataType::OTHER,
              CacheWarmupPolicy::CacheWarmupPolicy_Disable,
              false)) {
    }
    ~TestIndexTranslator() override = default;

    size_t
    num_cells() const override {
        return 1;
    }

    cid_t
    cell_id_of(uid_t uid) const override {
        return uid;
    }

    std::pair<ResourceUsage, ResourceUsage>
    estimated_byte_size_of_cell(cid_t cid) const override {
        return {{0, 0}, {0, 0}};
    }

    int64_t
    cells_storage_bytes(const std::vector<cid_t>& cids) const override {
        return 0;
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::IndexBase>>>
    get_cells(const std::vector<cid_t>& cids) override {
        std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::IndexBase>>>
            res;
        res.reserve(cids.size());
        for (auto cid : cids) {
            AssertInfo(cid == 0, "TestIndexTranslator assumes only one cell.");
            res.emplace_back(cid, std::move(index_));
        }
        return res;
    }

 private:
    std::string key_;
    std::unique_ptr<milvus::index::IndexBase> index_;
    milvus::cachinglayer::Meta meta_;
};

inline index::CacheIndexBasePtr
CreateTestCacheIndex(std::string key,
                     std::unique_ptr<milvus::index::IndexBase>&& index) {
    std::unique_ptr<milvus::cachinglayer::Translator<milvus::index::IndexBase>>
        translator = std::make_unique<TestIndexTranslator>(std::move(key),
                                                           std::move(index));
    return milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(translator));
}

inline std::map<std::string, std::string>
GenIndexParams(const milvus::index::IndexBase* index) {
    std::map<std::string, std::string> index_params;
    index_params["index_type"] = index->Type();
    return index_params;
}

}  // namespace milvus
