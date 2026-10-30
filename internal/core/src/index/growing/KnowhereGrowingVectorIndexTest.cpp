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

#include "index/growing/KnowhereGrowingVectorIndex.h"
#include "knowhere/index/index_factory.h"
#include "segcore/indexing/GrowingIndexSet.h"

namespace milvus::index::test {
namespace {

// Pause inside native Add while the owner's writer lock is held.
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
    WriterPause add;
    std::atomic<int> adds{0};
    std::atomic<bool> fail_publication_after_add{false};
    std::atomic<bool> short_count{false};
};

// Keep the real concurrent IVFFlat engine and IdMap. Control Add entry and
// the publication's Count validation without production test hooks.
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
    }

    knowhere::Status
    Build(const knowhere::DataSetPtr dataset,
          std::shared_ptr<knowhere::Config> cfg,
          bool use_pool) override {
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

    knowhere::expected<knowhere::DataSetPtr>
    GetVectorByIds(const knowhere::DataSetPtr dataset,
                   milvus::OpContext* ctx) const override {
        return inner_.Node()->GetVectorByIds(dataset, ctx);
    }

    bool
    HasRawData(const std::string& metric) const override {
        return inner_.Node()->HasRawData(metric);
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
        return inner_.Node()->GetIdMap();
    }

    const knowhere::IdMap&
    GetIdMap() const override {
        return inner_.Node()->GetIdMap();
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

    std::shared_ptr<NativeControl> control_ = std::make_shared<NativeControl>();
    std::shared_ptr<TestVectorSource> source_ =
        std::make_shared<TestVectorSource>();
    std::unique_ptr<segcore::GrowingIndexSet> indexes_ =
        std::make_unique<segcore::GrowingIndexSet>();
    KnowhereGrowingVectorIndex<float>* owner_{nullptr};
    const FieldId field_{101};
};

TEST_F(KnowhereGrowingVectorIndexTest, PinDoesNotWaitForAdd) {
    FirstBatch();
    control_->add.Arm();
    auto writer = std::async(std::launch::async, [&] {
        const bool valid = true;
        owner_->Append(
            4, VectorBatch<float>{1, source_->values.data() + 4, 2, &valid});
    });
    const bool entered = control_->add.WaitUntilEntered();
    auto query = std::async(std::launch::async, [&] { return Pin(); });
    const auto status = query.wait_for(std::chrono::seconds(2));
    // Always unblock and join before asserting, including on the old code.
    control_->add.Release();
    writer.get();
    const auto pin = query.get();
    ASSERT_TRUE(entered);
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(pin.CoveredRowEnd(), 4);
    EXPECT_EQ(pin.Reader().Count(), 2);
    EXPECT_EQ(Pin().CoveredRowEnd(), 5);
}

TEST_F(KnowhereGrowingVectorIndexTest, PublicationRetryStaysOnFlush) {
    FirstBatch();
    const auto first = Pin();
    const bool valid = true;
    const VectorBatch<float> tail{1, source_->values.data() + 4, 2, &valid};
    control_->fail_publication_after_add = true;
    EXPECT_ANY_THROW(owner_->Append(4, tail));
    control_->short_count = false;
    control_->fail_publication_after_add = false;
    // Pin must neither retry publication nor advance the published prefix.
    EXPECT_EQ(&Pin().Reader(), &first.Reader());
    EXPECT_EQ(Pin().CoveredRowEnd(), 4);
    owner_->Flush();
    EXPECT_EQ(control_->adds.load(), 1);
    EXPECT_EQ(Pin().CoveredRowEnd(), 5);
}

}  // namespace
}  // namespace milvus::index::test
