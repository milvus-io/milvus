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
#include <utility>
#include <vector>

#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/type_c.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "cachinglayer/lrucache/DList.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/IVectorReader.h"
#include "index_test_utils.h"
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
    get_cells(milvus::OpContext* ctx, const std::vector<cid_t>& cids) override {
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
                             std::vector<std::unique_ptr<GroupChunk>>&& chunks,
                             segcore::storagev2translator::SkipMetricsByField
                                 skip_metrics_by_field = {})
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
        // Same install point as production: test fixtures inherit the
        // metrics/cell alignment rule instead of bypassing it.
        meta_.InstallSkipMetrics(
            std::move(skip_metrics_by_field), num_cells_, key);
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
    get_cells(milvus::OpContext* ctx, const std::vector<cid_t>& cids) override {
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

class TestIndexTranslator : public Translator<milvus::index::IIndexReaderBase> {
 public:
    TestIndexTranslator(
        std::string key,
        std::unique_ptr<milvus::index::IIndexReaderBase>&& index)
        : TestIndexTranslator(std::move(key), std::move(index), nullptr) {
    }

    TestIndexTranslator(
        std::string key,
        std::unique_ptr<milvus::index::IIndexReaderBase>&& index,
        milvus::OpContext** observed_ctx)
        : Translator<milvus::index::IIndexReaderBase>(),
          key_(key),
          index_(std::move(index)),
          observed_ctx_(observed_ctx),
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

    std::vector<
        std::pair<cid_t, std::unique_ptr<milvus::index::IIndexReaderBase>>>
    get_cells(milvus::OpContext* ctx, const std::vector<cid_t>& cids) override {
        if (observed_ctx_ != nullptr) {
            *observed_ctx_ = ctx;
        }
        std::vector<
            std::pair<cid_t, std::unique_ptr<milvus::index::IIndexReaderBase>>>
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
    std::unique_ptr<milvus::index::IIndexReaderBase> index_;
    milvus::OpContext** observed_ctx_{nullptr};
    milvus::cachinglayer::Meta meta_;
};

inline std::shared_ptr<cachinglayer::CacheSlot<index::IIndexReaderBase>>
CreateTestCacheIndex(std::string key,
                     std::unique_ptr<milvus::index::IIndexReaderBase>&& index) {
    std::unique_ptr<
        milvus::cachinglayer::Translator<milvus::index::IIndexReaderBase>>
        translator = std::make_unique<TestIndexTranslator>(std::move(key),
                                                           std::move(index));
    return milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(translator));
}

inline std::shared_ptr<cachinglayer::CacheSlot<index::IIndexReaderBase>>
CreateTestCacheIndex(std::string key,
                     std::unique_ptr<milvus::index::IIndexReaderBase>&& index,
                     milvus::OpContext** observed_ctx) {
    std::unique_ptr<
        milvus::cachinglayer::Translator<milvus::index::IIndexReaderBase>>
        translator = std::make_unique<TestIndexTranslator>(
            std::move(key), std::move(index), observed_ctx);
    return milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(translator));
}

inline std::map<std::string, std::string>
GenIndexParams(const milvus::index::IIndexReaderBase* reader,
               const std::string& family = index::families::kSort) {
    std::map<std::string, std::string> params;
    if (auto vector = dynamic_cast<const index::IVectorReader*>(reader)) {
        params[index::INDEX_TYPE] = vector->KnowhereIndexType();
        params[index::METRIC_TYPE] = vector->Metric();
        return params;
    }
    const std::map<std::string, std::string> scalar_types{
        {index::families::kSort, index::ASCENDING_SORT},
        {index::families::kBitmap, index::BITMAP_INDEX_TYPE},
        {index::families::kInverted, index::INVERTED_INDEX_TYPE},
        {index::families::kMarisa, index::MARISA_TRIE},
        {index::families::kFmIndex, index::FMINDEX_INDEX_TYPE},
        {index::families::kNgram, index::NGRAM_INDEX_TYPE},
        {index::families::kRTree, index::RTREE_INDEX_TYPE},
        {index::families::kJsonFlat, "JSON_FLAT"},
    };
    const auto found = scalar_types.find(family);
    AssertInfo(found != scalar_types.end(),
               "test scalar reader requires an explicit concrete family");
    params[index::INDEX_TYPE] = found->second;
    return params;
}

}  // namespace milvus
