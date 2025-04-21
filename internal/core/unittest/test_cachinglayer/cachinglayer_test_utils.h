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

#include "cachinglayer/Translator.h"
#include "common/Chunk.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "cachinglayer/lrucache/DList.h"

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
          meta_(segcore::storagev1translator::CTMeta(StorageType::MEMORY)) {
        meta_.num_rows_until_chunk_.reserve(num_cells_ + 1);
        meta_.num_rows_until_chunk_.push_back(0);
        int total_rows = 0;
        for (int i = 0; i < num_cells_; ++i) {
            meta_.num_rows_until_chunk_.push_back(
                meta_.num_rows_until_chunk_[i] + num_rows_per_chunk[i]);
            total_rows += num_rows_per_chunk[i];
        }
        key_ = key;
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

    ResourceUsage
    estimated_byte_size_of_cell(cid_t cid) const override {
        return ResourceUsage(0, 0);
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

namespace cachinglayer::internal {
class DListTestFriend {
 public:
    static ResourceUsage
    get_used_memory(const DList& dlist) {
        return dlist.used_memory_.load();
    }
    static ResourceUsage
    get_max_memory(const DList& dlist) {
        std::lock_guard lock(dlist.list_mtx_);
        return dlist.max_memory_;
    }
    static ListNode*
    get_head(const DList& dlist) {
        std::lock_guard lock(dlist.list_mtx_);
        return dlist.head_;
    }
    static ListNode*
    get_tail(const DList& dlist) {
        std::lock_guard lock(dlist.list_mtx_);
        return dlist.tail_;
    }
    static void
    test_push_head(DList* dlist, ListNode* node) {
        std::lock_guard lock(dlist->list_mtx_);
        dlist->pushHead(node);
    }
    static void
    test_pop_item(DList* dlist, ListNode* node) {
        std::lock_guard lock(dlist->list_mtx_);
        dlist->popItem(node);
    }
    static void
    test_add_used_memory(DList* dlist, const ResourceUsage& size) {
        std::lock_guard lock(dlist->list_mtx_);
        dlist->used_memory_ += size;
    }

    // nodes are from tail to head
    static void
    verify_list(DList* dlist, std::vector<ListNode*> nodes) {
        std::lock_guard lock(dlist->list_mtx_);
        EXPECT_EQ(nodes.front(), dlist->tail_);
        EXPECT_EQ(nodes.back(), dlist->head_);
        for (size_t i = 0; i < nodes.size(); ++i) {
            auto current = nodes[i];
            auto expected_prev = i == 0 ? nullptr : nodes[i - 1];
            auto expected_next = i == nodes.size() - 1 ? nullptr : nodes[i + 1];
            EXPECT_EQ(current->prev_, expected_prev);
            EXPECT_EQ(current->next_, expected_next);
        }
    }

    static void
    verify_integrity(DList* dlist) {
        std::lock_guard lock(dlist->list_mtx_);

        ResourceUsage total_size;
        EXPECT_EQ(dlist->tail_->prev_, nullptr);
        ListNode* current = dlist->tail_;
        ListNode* prev = nullptr;

        while (current != nullptr) {
            EXPECT_EQ(current->prev_, prev);
            total_size += current->size();
            prev = current;
            current = current->next_;
        }

        EXPECT_EQ(prev, dlist->head_);
        EXPECT_EQ(dlist->head_->next_, nullptr);

        EXPECT_EQ(total_size, dlist->used_memory_.load());
    }
};
}  // namespace cachinglayer::internal

}  // namespace milvus
