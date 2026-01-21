// Copyright (C) 2019-2020 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <folly/CancellationToken.h>
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Chunk.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/segment_c.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::cachinglayer;

// C API Tests for Load Cancellation Source Management
TEST(LoadCancellationCAPI, NewAndReleaseSource) {
    CLoadCancellationSource source = NewLoadCancellationSource();
    ASSERT_NE(source, nullptr);
    ReleaseLoadCancellationSource(source);
}

TEST(LoadCancellationCAPI, CancelSource) {
    CLoadCancellationSource source = NewLoadCancellationSource();
    ASSERT_NE(source, nullptr);
    CancelLoadCancellationSource(source);
    auto* folly_source = static_cast<folly::CancellationSource*>(source);
    EXPECT_TRUE(folly_source->getToken().isCancellationRequested());
    ReleaseLoadCancellationSource(source);
}

TEST(LoadCancellationCAPI, CancelNullSource) {
    CancelLoadCancellationSource(nullptr);
}

TEST(LoadCancellationCAPI, ReleaseNullSource) {
    ReleaseLoadCancellationSource(nullptr);
}

TEST(LoadCancellationCAPI, MultipleCancellations) {
    CLoadCancellationSource source = NewLoadCancellationSource();
    CancelLoadCancellationSource(source);
    CancelLoadCancellationSource(source);
    auto* folly_source = static_cast<folly::CancellationSource*>(source);
    EXPECT_TRUE(folly_source->getToken().isCancellationRequested());
    ReleaseLoadCancellationSource(source);
}

// Test for Segment CreateTextIndex cancellation (sealed segment)
TEST(LoadCancellationSegment, CreateTextIndexCancellationSealed) {
    auto schema = std::make_shared<Schema>();
    auto text_fid = schema->AddDebugField("text", DataType::VARCHAR);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto segment = CreateSealedSegment(schema, nullptr, -1, SegcoreConfig::default_config(), true);
    folly::CancellationSource source;
    source.requestCancellation();
    OpContext op_ctx(source.getToken());
    EXPECT_THROW({
        try { segment->CreateTextIndex(text_fid, &op_ctx); }
        catch (const SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
            throw;
        }
    }, SegcoreError);
}

// Test for Segment CreateTextIndex cancellation (growing segment)
TEST(LoadCancellationSegment, CreateTextIndexCancellationGrowing) {
    auto schema = std::make_shared<Schema>();
    auto text_fid = schema->AddDebugField("text", DataType::VARCHAR);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto segment = CreateGrowingSegment(schema, nullptr, 1, SegcoreConfig::default_config());
    folly::CancellationSource source;
    source.requestCancellation();
    OpContext op_ctx(source.getToken());
    EXPECT_THROW({
        try { segment->CreateTextIndex(text_fid, &op_ctx); }
        catch (const SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
            throw;
        }
    }, SegcoreError);
}

// Note: CreateTextIndex with null context is implicitly tested by the cancellation tests
// since the cancellation check happens before the analyzer validation. If cancellation
// doesn't throw, the function continues and would need proper analyzer setup to succeed.

// Test cancellation during loop operation
TEST(LoadCancellationUtility, CancellationDuringLoop) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    int iterations = 0;
    bool caught = false;
    try {
        for (int i = 0; i < 100; ++i) {
            CheckCancellation(&op_ctx, 123, i, "LoopOp");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            iterations++;
            if (i == 4) source.requestCancellation();
        }
    } catch (const SegcoreError& e) {
        caught = true;
        EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_TRUE(caught);
    EXPECT_EQ(iterations, 5);
}

// Test concurrent cancellation from another thread
TEST(LoadCancellationUtility, ConcurrentCancellation) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    std::atomic<int> iterations{0};
    std::atomic<bool> caught{false};
    std::thread worker([&]() {
        try {
            for (int i = 0; i < 1000; ++i) {
                CheckCancellation(&op_ctx, 456, fmt::format("Iter{}", i));
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                iterations++;
            }
        } catch (const SegcoreError& e) {
            caught = true;
            EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    source.requestCancellation();
    worker.join();
    EXPECT_TRUE(caught.load());
    EXPECT_GT(iterations.load(), 0);
    EXPECT_LT(iterations.load(), 1000);
}

// Test that token is shared across contexts
TEST(LoadCancellationUtility, TokenSharedAcrossContexts) {
    folly::CancellationSource source;
    OpContext op_ctx1(source.getToken());
    OpContext op_ctx2(source.getToken());
    source.requestCancellation();
    EXPECT_THROW(CheckCancellation(&op_ctx1, 1, "T1"), SegcoreError);
    EXPECT_THROW(CheckCancellation(&op_ctx2, 2, "T2"), SegcoreError);
}

// Test error message format
TEST(LoadCancellationUtility, ErrorMessageFormat) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    source.requestCancellation();
    try {
        CheckCancellation(&op_ctx, 12345, "TestOp");
        FAIL() << "Expected exception";
    } catch (const SegcoreError& e) {
        std::string msg(e.what());
        EXPECT_TRUE(msg.find("TestOp") != std::string::npos);
        EXPECT_TRUE(msg.find("12345") != std::string::npos);
    }
    try {
        CheckCancellation(&op_ctx, 12345, 67890, "FieldOp");
        FAIL() << "Expected exception";
    } catch (const SegcoreError& e) {
        std::string msg(e.what());
        EXPECT_TRUE(msg.find("FieldOp") != std::string::npos);
        EXPECT_TRUE(msg.find("12345") != std::string::npos);
        EXPECT_TRUE(msg.find("67890") != std::string::npos);
    }
}

// OpContext tests
TEST(OpContextTest, DefaultConstructor) {
    OpContext op_ctx;
    EXPECT_FALSE(op_ctx.cancellation_token.isCancellationRequested());
}

TEST(OpContextTest, ConstructorWithToken) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    EXPECT_FALSE(op_ctx.cancellation_token.isCancellationRequested());
    source.requestCancellation();
    EXPECT_TRUE(op_ctx.cancellation_token.isCancellationRequested());
}

TEST(OpContextTest, TokenSharedFromSameSource) {
    // OpContext is non-copyable, but multiple contexts can share the same token
    folly::CancellationSource source;
    OpContext op_ctx1(source.getToken());
    OpContext op_ctx2(source.getToken());
    source.requestCancellation();
    EXPECT_TRUE(op_ctx1.cancellation_token.isCancellationRequested());
    EXPECT_TRUE(op_ctx2.cancellation_token.isCancellationRequested());
}

// Integration test: Multiple cancellation check points
TEST(LoadCancellationIntegration, MultipleCheckPoints) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    std::vector<std::string> passed;
    int64_t seg_id = 999, field_id = 100;

    // No cancellation - all checkpoints pass
    EXPECT_NO_THROW({
        CheckCancellation(&op_ctx, seg_id, "BeforeLoad"); passed.push_back("BeforeLoad");
        CheckCancellation(&op_ctx, seg_id, field_id, "LoadFieldData"); passed.push_back("LoadFieldData");
        CheckCancellation(&op_ctx, seg_id, field_id, "LoadIndex"); passed.push_back("LoadIndex");
        CheckCancellation(&op_ctx, seg_id, field_id, "CreateTextIndex"); passed.push_back("CreateTextIndex");
        CheckCancellation(&op_ctx, seg_id, "AfterLoad"); passed.push_back("AfterLoad");
    });
    EXPECT_EQ(passed.size(), 5);

    // Pre-cancelled - first checkpoint fails
    passed.clear();
    folly::CancellationSource source2;
    OpContext op_ctx2(source2.getToken());
    source2.requestCancellation();
    try {
        CheckCancellation(&op_ctx2, seg_id, "BeforeLoad");
        FAIL() << "Expected exception";
    } catch (const SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_TRUE(passed.empty());
}

// Test Translator with Cancellation Support
class SlowChunkTranslator : public cachinglayer::Translator<milvus::Chunk> {
 public:
    SlowChunkTranslator(size_t num_chunks, std::chrono::milliseconds delay, std::string key)
        : num_chunks_(num_chunks), delay_(delay), key_(std::move(key)),
          meta_(storagev1translator::CTMeta(
              cachinglayer::StorageType::MEMORY,
              cachinglayer::CellIdMappingMode::IDENTICAL,
              cachinglayer::CellDataType::SCALAR_FIELD,
              CacheWarmupPolicy::CacheWarmupPolicy_Disable, true)),
          chunks_loaded_(0), was_cancelled_(false) {
        meta_.num_rows_until_chunk_.push_back(0);
        for (size_t i = 0; i < num_chunks_; ++i) {
            meta_.num_rows_until_chunk_.push_back(meta_.num_rows_until_chunk_[i] + 100);
        }
        storagev1translator::virtual_chunk_config(100 * num_chunks_, num_chunks_,
            meta_.num_rows_until_chunk_, meta_.virt_chunk_order_, meta_.vcid_to_cid_arr_);
    }
    ~SlowChunkTranslator() override = default;
    size_t num_cells() const override { return num_chunks_; }
    cachinglayer::cid_t cell_id_of(cachinglayer::uid_t uid) const override { return uid; }
    std::pair<cachinglayer::ResourceUsage, cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(cachinglayer::cid_t) const override { return {{0, 0}, {0, 0}}; }
    int64_t cells_storage_bytes(const std::vector<cachinglayer::cid_t>&) const override { return 0; }
    const std::string& key() const override { return key_; }
    cachinglayer::Meta* meta() override { return &meta_; }

    std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
    get_cells(milvus::OpContext* ctx, const std::vector<cachinglayer::cid_t>& cids) override {
        std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>> res;
        res.reserve(cids.size());
        for (auto cid : cids) {
            if (ctx && ctx->cancellation_token.isCancellationRequested()) {
                was_cancelled_ = true;
                throw SegcoreError(ErrorCode::FollyCancel, fmt::format("cancelled at chunk {}", cid));
            }
            std::this_thread::sleep_for(delay_);
            auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
            auto chunk = std::make_unique<FixedWidthChunk>(100, 1, nullptr, 0, sizeof(int64_t), false, guard);
            res.emplace_back(cid, std::move(chunk));
            chunks_loaded_++;
        }
        return res;
    }
    size_t chunks_loaded() const { return chunks_loaded_; }
    bool was_cancelled() const { return was_cancelled_; }

 private:
    size_t num_chunks_;
    std::chrono::milliseconds delay_;
    std::string key_;
    storagev1translator::CTMeta meta_;
    std::atomic<size_t> chunks_loaded_;
    std::atomic<bool> was_cancelled_;
};

// Test that translator respects cancellation during chunk loading
TEST(LoadCancellationTranslator, RespectsCancellation) {
    auto translator = std::make_unique<SlowChunkTranslator>(
        10, std::chrono::milliseconds(50), "slow_translator");
    auto* raw = translator.get();
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    std::thread load_thread([&]() {
        try {
            std::vector<cachinglayer::cid_t> cids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            raw->get_cells(&op_ctx, cids);
        } catch (const SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    source.requestCancellation();
    load_thread.join();
    EXPECT_TRUE(raw->was_cancelled());
    EXPECT_LT(raw->chunks_loaded(), 10);
}

// Test that translator completes without cancellation
TEST(LoadCancellationTranslator, CompletesWithoutCancellation) {
    auto translator = std::make_unique<SlowChunkTranslator>(
        5, std::chrono::milliseconds(10), "complete_translator");
    auto* raw = translator.get();
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    std::vector<cachinglayer::cid_t> cids = {0, 1, 2, 3, 4};
    EXPECT_NO_THROW(raw->get_cells(&op_ctx, cids));
    EXPECT_EQ(raw->chunks_loaded(), 5);
    EXPECT_FALSE(raw->was_cancelled());
}

// Test that translator works with null context
TEST(LoadCancellationTranslator, WorksWithNullContext) {
    auto translator = std::make_unique<SlowChunkTranslator>(
        3, std::chrono::milliseconds(5), "null_ctx_translator");
    auto* raw = translator.get();
    std::vector<cachinglayer::cid_t> cids = {0, 1, 2};
    EXPECT_NO_THROW(raw->get_cells(nullptr, cids));
    EXPECT_EQ(raw->chunks_loaded(), 3);
    EXPECT_FALSE(raw->was_cancelled());
}

// Test cancellation before load starts
TEST(LoadCancellationTranslator, CancellationBeforeLoad) {
    auto translator = std::make_unique<SlowChunkTranslator>(
        5, std::chrono::milliseconds(10), "pre_cancel_translator");
    auto* raw = translator.get();
    folly::CancellationSource source;
    source.requestCancellation();
    OpContext op_ctx(source.getToken());
    std::vector<cachinglayer::cid_t> cids = {0, 1, 2, 3, 4};
    EXPECT_THROW(raw->get_cells(&op_ctx, cids), SegcoreError);
    EXPECT_EQ(raw->chunks_loaded(), 0);
    EXPECT_TRUE(raw->was_cancelled());
}

// Test cancellation at specific checkpoint
TEST(LoadCancellationTranslator, CancellationAtMiddleChunk) {
    auto translator = std::make_unique<SlowChunkTranslator>(
        10, std::chrono::milliseconds(20), "middle_cancel_translator");
    auto* raw = translator.get();
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    std::atomic<bool> started{false};
    std::thread load_thread([&]() {
        started = true;
        try {
            std::vector<cachinglayer::cid_t> cids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            raw->get_cells(&op_ctx, cids);
        } catch (const SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
        }
    });
    // Wait for load to start
    while (!started) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Wait for approximately 3 chunks to load then cancel
    std::this_thread::sleep_for(std::chrono::milliseconds(70));
    source.requestCancellation();
    load_thread.join();
    EXPECT_TRUE(raw->was_cancelled());
    // Should have loaded some chunks but not all
    EXPECT_GT(raw->chunks_loaded(), 0);
    EXPECT_LT(raw->chunks_loaded(), 10);
}
