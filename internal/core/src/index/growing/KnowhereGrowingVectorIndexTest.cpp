// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This translation unit constructs an IndexNode subclass passed to the native
// knowhere library. Match its native object layout, including metric caches,
// before any knowhere header is included.
#ifndef NOT_COMPILE_FOR_SWIG
#define NOT_COMPILE_FOR_SWIG
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "common/Utils.h"
#include "index/growing/KnowhereGrowingVectorIndex.h"
#include "knowhere/index/index_factory.h"
#include "segcore/indexing/GrowingIndexSet.h"

namespace milvus::index::test {
namespace {

// Pause inside the native Build/Add call while the owner's writer is held.
// A failed assertion must still release the writer before joining its future.
class WriterPause {
 public:
    void
    Arm() {
        std::lock_guard lock(mutex_);
        armed_ = true;
        entered_ = false;
    }

    void
    Enter() {
        std::unique_lock lock(mutex_);
        if (!armed_) {
            return;
        }
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [&] { return !armed_; });
    }

    bool
    WaitUntilEntered() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(
            lock, std::chrono::seconds(5), [&] { return entered_; });
    }

    void
    Release() {
        std::lock_guard lock(mutex_);
        armed_ = false;
        cv_.notify_all();
    }

 private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool armed_{false};
    bool entered_{false};
};

struct NativeControl {
    WriterPause build;
    WriterPause add;
    std::atomic<int> builds{0};
    std::atomic<int> adds{0};
    std::atomic<int> live_nodes{0};
    std::atomic<bool> fail_build{false};
    std::atomic<bool> fail_add{false};
    std::atomic<bool> fail_publication_after_add{false};
    std::atomic<bool> short_count{false};
};

// Keep the real concurrent IVFFlat engine and IdMap. Only native call entry
// and the publication's Count validation are controlled by this test adapter.
class ControlledVectorNode final : public knowhere::IndexNode {
 public:
    ControlledVectorNode(int32_t version,
                         std::shared_ptr<NativeControl> control)
        : knowhere::IndexNode(version),
          control_(std::move(control)),
          inner_(knowhere::IndexFactory::Instance()
                     .Create<float>(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                                    version)
                     .value()) {
        ++control_->live_nodes;
    }

    ~ControlledVectorNode() override {
        --control_->live_nodes;
    }

    knowhere::Status
    Build(const knowhere::DataSetPtr dataset,
          std::shared_ptr<knowhere::Config> cfg,
          bool use_pool) override {
        ++control_->builds;
        control_->build.Enter();
        if (control_->fail_build) {
            return knowhere::Status::malloc_error;
        }
        return inner_.Node()->Build(dataset, std::move(cfg), use_pool);
    }

    knowhere::Status
    Train(const knowhere::DataSetPtr dataset,
          std::shared_ptr<knowhere::Config> cfg,
          bool use_pool) override {
        return inner_.Node()->Train(dataset, std::move(cfg), use_pool);
    }

    knowhere::Status
    Add(const knowhere::DataSetPtr dataset,
        std::shared_ptr<knowhere::Config> cfg,
        bool use_pool) override {
        ++control_->adds;
        control_->add.Enter();
        if (control_->fail_add) {
            return knowhere::Status::malloc_error;
        }
        const auto status =
            inner_.Node()->Add(dataset, std::move(cfg), use_pool);
        if (status == knowhere::Status::success &&
            control_->fail_publication_after_add) {
            control_->short_count = true;
        }
        return status;
    }

    knowhere::expected<knowhere::DataSetPtr>
    Search(const knowhere::DataSetPtr dataset,
           std::unique_ptr<knowhere::Config> cfg,
           const knowhere::BitsetView& bitset,
           milvus::OpContext* ctx) const override {
        return inner_.Node()->Search(dataset, std::move(cfg), bitset, ctx);
    }

    knowhere::expected<std::vector<IteratorPtr>>
    AnnIterator(const knowhere::DataSetPtr dataset,
                std::unique_ptr<knowhere::Config> cfg,
                const knowhere::BitsetView& bitset,
                bool use_pool,
                milvus::OpContext* ctx) const override {
        return inner_.Node()->AnnIterator(
            dataset, std::move(cfg), bitset, use_pool, ctx);
    }

    knowhere::expected<knowhere::DataSetPtr>
    GetVectorByIds(const knowhere::DataSetPtr dataset,
                   milvus::OpContext* ctx) const override {
        return inner_.Node()->GetVectorByIds(dataset, ctx);
    }

    bool
    HasRawData(const std::string& metric) const override {
        return inner_.Node()->HasRawData(metric);
    }

    bool
    NeedBitsetExactCount() const override {
        return inner_.Node()->NeedBitsetExactCount();
    }

    knowhere::expected<knowhere::IndexNode::PreparedBitset>
    PrepareBitset(knowhere::BitsetView bitset) const override {
        // Preserve the native IdMap projection and backend-domain counts.
        return inner_.Node()->PrepareBitset(bitset);
    }

    knowhere::expected<knowhere::DataSetPtr>
    GetIndexMeta(std::unique_ptr<knowhere::Config> cfg) const override {
        return inner_.Node()->GetIndexMeta(std::move(cfg));
    }

    knowhere::Status
    Serialize(knowhere::BinarySet& binset) const override {
        return inner_.Node()->Serialize(binset);
    }

    knowhere::Status
    Deserialize(const knowhere::BinarySet& binset,
                std::shared_ptr<knowhere::Config> cfg) override {
        return inner_.Node()->Deserialize(binset, std::move(cfg));
    }

    knowhere::Status
    DeserializeFromFile(const std::string& path,
                        std::shared_ptr<knowhere::Config> cfg) override {
        return inner_.Node()->DeserializeFromFile(path, std::move(cfg));
    }

    std::unique_ptr<knowhere::BaseConfig>
    CreateConfig() const override {
        return inner_.Node()->CreateConfig();
    }

    int64_t
    Dim() const override {
        return inner_.Dim();
    }

    int64_t
    Size() const override {
        return inner_.Size();
    }

    int64_t
    Count() const override {
        return control_->short_count ? 0 : inner_.Count();
    }

    std::string
    Type() const override {
        return inner_.Type();
    }

    knowhere::IdMap&
    GetIdMap() override {
        return inner_.GetIdMap();
    }

    const knowhere::IdMap&
    GetIdMap() const override {
        return inner_.GetIdMap();
    }

    knowhere::Status
    FinalizeIdMap() override {
        return inner_.Node()->FinalizeIdMap();
    }

 private:
    std::shared_ptr<NativeControl> control_;
    knowhere::Index<knowhere::IndexNode> inner_;
};

class TestVectorSource final : public GrowingVectorSource<float> {
 public:
    const std::array<float, 8> values{0, 0, 2, 2, 4, 4, 6, 6};

    std::span<const float>
    ContiguousRows(int64_t begin, int64_t count) const override {
        return {values.data() + begin * 2, static_cast<size_t>(count * 2)};
    }

    void
    CopyRows(int64_t begin, int64_t count, float* output) const override {
        const auto rows = ContiguousRows(begin, count);
        std::copy(rows.begin(), rows.end(), output);
    }

    const float*
    Row(int64_t offset) const override {
        return values.data() + offset * 2;
    }
};

class KnowhereGrowingVectorIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        static std::atomic<int> next_id{0};
        const auto type =
            "CONTROLLED_GROWING_VECTOR_" + std::to_string(next_id.fetch_add(1));
        knowhere::IndexFactory::Instance().Register<float>(
            type,
            [control = control_](const int32_t& version,
                                 const knowhere::Object&) {
                return knowhere::Index<knowhere::IndexNode>(
                    knowhere::Index<ControlledVectorNode>::Create(version,
                                                                  control));
            },
            knowhere::feature::FLOAT32);
        auto owner = std::make_unique<KnowhereGrowingVectorIndex<float>>(
            DataType::VECTOR_FLOAT,
            type,
            knowhere::metric::L2,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            2,
            2,
            knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                           {knowhere::meta::DIM, "2"},
                           {knowhere::indexparam::NLIST, "1"},
                           {knowhere::meta::NUM_BUILD_THREAD, "1"}},
            knowhere::Json{{knowhere::indexparam::NPROBE, "1"}},
            source_,
            false);
        owner_ = owner.get();
        segcore::GrowingIndexSet::AppenderMap staged;
        staged.emplace(field_,
                       segcore::GrowingIndexSet::Appender(
                           field_, ReaderCaps{}, std::move(owner)));
        indexes_->RegisterBatch(std::move(staged));
    }

    GrowingIndexSnapshotPin
    Pin() const {
        // Exercise the same maintenance hook + pin sequence as a Segment.
        return indexes_->PinSnapshot(field_);
    }

    void
    FirstBatch() {
        const std::array<bool, 4> valid{true, false, true, false};
        owner_->Append(
            0, VectorBatch<float>{4, source_->values.data(), 2, valid.data()});
    }

    SearchResult
    Search(const GrowingIndexSnapshotPin& pin) const {
        const auto* reader = dynamic_cast<const IVectorReader*>(&pin.Reader());
        const VectorSearchParams params{
            .search_params_ = {{knowhere::indexparam::NPROBE, "1"}},
            .metric_type_ = knowhere::metric::L2,
            .topk_ = 4};
        SearchResult result;
        reader->Search(GenDataset(1, 2, source_->values.data()),
                       params,
                       BitsetView{},
                       nullptr,
                       result);
        return result;
    }

    std::shared_ptr<NativeControl> control_ = std::make_shared<NativeControl>();
    std::shared_ptr<TestVectorSource> source_ =
        std::make_shared<TestVectorSource>();
    std::unique_ptr<segcore::GrowingIndexSet> indexes_ =
        std::make_unique<segcore::GrowingIndexSet>();
    KnowhereGrowingVectorIndex<float>* owner_{nullptr};
    const FieldId field_{101};
};

TEST_F(KnowhereGrowingVectorIndexTest, PinDoesNotWaitForColdBuild) {
    control_->build.Arm();
    auto writer = std::async(std::launch::async, [&] { FirstBatch(); });
    const bool entered = control_->build.WaitUntilEntered();
    auto query = std::async(std::launch::async, [&] { return Pin(); });
    const auto status = query.wait_for(std::chrono::seconds(2));
    control_->build.Release();
    writer.get();
    const auto pin = query.get();
    EXPECT_TRUE(entered);
    EXPECT_EQ(status, std::future_status::ready);
    // With no publication, SearchOnGrowing keeps its complete raw generation.
    EXPECT_FALSE(static_cast<bool>(pin));
    EXPECT_EQ(Pin().CoveredRowEnd(), 4);
}

TEST_F(KnowhereGrowingVectorIndexTest,
       SearchDoesNotWaitForAddAndKeepsNullablePrefix) {
    FirstBatch();
    const auto first = Pin();
    control_->add.Arm();
    auto writer = std::async(std::launch::async, [&] {
        const std::array<bool, 3> valid{false, true, true};
        owner_->Append(
            4,
            VectorBatch<float>{3, source_->values.data() + 4, 2, valid.data()});
    });
    const bool entered = control_->add.WaitUntilEntered();
    auto query = std::async(std::launch::async, [&] {
        const auto pin = Pin();
        EXPECT_EQ(pin.CoveredRowEnd(), 4);
        EXPECT_EQ(pin.Reader().Count(), 2);
        const auto* reader = dynamic_cast<const IVectorReader*>(&pin.Reader());
        EXPECT_TRUE(reader->IsRowValid(2));
        EXPECT_FALSE(reader->IsRowValid(3));
        EXPECT_FALSE(reader->IsRowValid(5));
        return Search(pin);
    });
    const auto status = query.wait_for(std::chrono::seconds(2));
    control_->add.Release();
    writer.get();
    const auto result = query.get();
    EXPECT_TRUE(entered);
    EXPECT_EQ(status, std::future_status::ready);
    for (const auto row : result.seg_offsets_) {
        EXPECT_TRUE(row == INVALID_SEG_OFFSET || row == 0 || row == 2);
    }
    EXPECT_EQ(first.CoveredRowEnd(), 4);
    const auto latest = Pin();
    EXPECT_EQ(latest.CoveredRowEnd(), 7);
    EXPECT_EQ(latest.Reader().Count(), 4);
    const auto* reader = dynamic_cast<const IVectorReader*>(&latest.Reader());
    EXPECT_FALSE(reader->IsRowValid(4));
    EXPECT_TRUE(reader->IsRowValid(5));
    EXPECT_TRUE(reader->IsRowValid(6));
}

TEST_F(KnowhereGrowingVectorIndexTest, PublicationRetryStaysOnAppendAndFlush) {
    FirstBatch();
    const auto first = Pin();
    const bool valid = true;
    const VectorBatch<float> tail{1, source_->values.data() + 4, 2, &valid};
    control_->fail_publication_after_add = true;
    EXPECT_ANY_THROW(owner_->Append(4, tail));
    EXPECT_EQ(control_->adds.load(), 1);
    control_->short_count = false;
    control_->fail_publication_after_add = false;
    EXPECT_EQ(&Pin().Reader(), &first.Reader());
    owner_->Append(4, tail);
    EXPECT_EQ(control_->adds.load(), 1);
    EXPECT_EQ(Pin().CoveredRowEnd(), 5);

    control_->fail_publication_after_add = true;
    EXPECT_ANY_THROW(owner_->Append(5, tail));
    control_->short_count = false;
    control_->fail_publication_after_add = false;
    EXPECT_EQ(Pin().CoveredRowEnd(), 5);
    owner_->Flush();
    EXPECT_EQ(control_->adds.load(), 2);
    EXPECT_EQ(Pin().CoveredRowEnd(), 6);
}

TEST_F(KnowhereGrowingVectorIndexTest, PublishedPoisonRejectsNewQueries) {
    FirstBatch();
    const auto first = Pin();
    control_->fail_add = true;
    const bool valid = true;
    const VectorBatch<float> tail{1, source_->values.data() + 4, 2, &valid};
    EXPECT_ANY_THROW(owner_->Append(4, tail));
    EXPECT_ANY_THROW(static_cast<void>(Pin()));
    EXPECT_ANY_THROW(owner_->Flush());
    EXPECT_ANY_THROW(owner_->Append(4, tail));
    EXPECT_EQ(first.CoveredRowEnd(), 4);
    EXPECT_EQ(first.Reader().Count(), 2);
    // The adapter failed before native Add. A pre-existing pin remains usable;
    // arbitrary native partial mutation is not claimed to be recoverable.
    EXPECT_NO_THROW(static_cast<void>(Search(first)));
}

TEST_F(KnowhereGrowingVectorIndexTest, ColdFailureRetainsSourceForFlushRetry) {
    control_->fail_build = true;
    FirstBatch();
    EXPECT_FALSE(static_cast<bool>(Pin()));
    EXPECT_NO_THROW(owner_->CommitIfNeeded());
    control_->fail_build = false;
    owner_->Flush();
    EXPECT_EQ(control_->builds.load(), 2);
    EXPECT_EQ(Pin().CoveredRowEnd(), 4);
}

TEST_F(KnowhereGrowingVectorIndexTest,
       IteratorKeepsPinnedEngineAfterOwnerTeardown) {
    FirstBatch();
    auto pin = Pin();
    const auto* reader = dynamic_cast<const IVectorReader*>(&pin.Reader());
    TargetBitmap prefix(static_cast<size_t>(pin.CoveredRowEnd()), false);
    const VectorSearchParams params{
        .search_params_ = {{knowhere::indexparam::NPROBE, "1"}},
        .metric_type_ = knowhere::metric::L2,
        .topk_ = 4};
    auto iterators = reader->Iterators(GenDataset(1, 2, source_->values.data()),
                                       reader->PrepareSearchParams(params),
                                       BitsetView(prefix),
                                       nullptr);
    ASSERT_TRUE(iterators.has_value()) << iterators.what();
    ASSERT_EQ(iterators.value().size(), 1);
    const bool valid = true;
    owner_->Append(
        4, VectorBatch<float>{1, source_->values.data() + 4, 2, &valid});
    indexes_.reset();
    EXPECT_EQ(control_->live_nodes.load(), 1);
    size_t visited = 0;
    auto& iterator = iterators.value().front();
    while (true) {
        auto has_next = iterator->HasNext();
        ASSERT_TRUE(has_next.has_value()) << has_next.what();
        if (!has_next.value()) {
            break;
        }
        auto next = iterator->Next();
        ASSERT_TRUE(next.has_value()) << next.what();
        EXPECT_TRUE(next.value().first == 0 || next.value().first == 2);
        ASSERT_LE(++visited, 2);
    }
    EXPECT_EQ(visited, 2);
    iterators.value().clear();
    pin = {};
    EXPECT_EQ(control_->live_nodes.load(), 0);
}

}  // namespace
}  // namespace milvus::index::test
