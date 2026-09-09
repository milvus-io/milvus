// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <future>
#include <numeric>
#include <random>
#include "arrow/filesystem/localfs.h"

#include "common/Slice.h"
#include "folly/ScopeGuard.h"
#include "folly/system/ThreadName.h"
#include "index/IndexFactory.h"
#include "index/VectorMemIndex.h"
#include "knowhere/version.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/LegacyIndexLoader.h"
#include "storage/LocalChunkManager.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/TmpPath.h"

namespace milvus::index {
namespace {

class VectorNativeFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    VectorNativeFileSystem()
        : arrow::fs::SubTreeFileSystem(
              "", std::make_shared<arrow::fs::LocalFileSystem>()) {
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
            "MILVUS_ASYNC"));
        return std::static_pointer_cast<arrow::io::RandomAccessFile>(
            files.at(path));
    }
    std::map<std::string, std::shared_ptr<test::ControlledDirectReadFile>>
        files;
};

class VectorLoadSource : public storage::LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        Observe();
        return LocalChunkManager::Read(path, data, bytes);
    }
    uint64_t
    Read(const std::string& path,
         uint64_t offset,
         void* data,
         uint64_t bytes) override {
        Observe();
        return LocalChunkManager::Read(path, offset, data, bytes);
    }
    void
    Observe() {
        ++reads;
        if (expect_async) {
            EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
                "MILVUS_ASYNC"));
            auto& admission = storage::LoadAdmissionController::GetInstance();
            const bool acquired = admission.TryAcquire(
                {0, 1}, storage::LoadAdmissionPriority::High);
            EXPECT_FALSE(acquired);
            if (acquired) {
                admission.Release({0, 1});
            }
        }
        if (on_read) {
            on_read();
        }
    }
    bool expect_async{false};
    std::atomic<int> reads{0};
    std::function<void()> on_read;
};

template <typename T>
class ObservedVectorIndex : public VectorMemIndex<T> {
 public:
    using VectorMemIndex<T>::VectorMemIndex;
    void
    LoadWithoutAssemble(const BinarySet& binary,
                        const Config& config) override {
        thread = folly::getCurrentThreadName().value_or("");
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const storage::LoadAdmissionRequest all{admission.CapacityBytes(), 1};
        const bool acquired =
            admission.TryAcquire(all, storage::LoadAdmissionPriority::High);
        EXPECT_TRUE(acquired);
        if (acquired) {
            admission.Release(all);
        }
        if (on_finalize) {
            on_finalize(binary);
        }
        VectorMemIndex<T>::LoadWithoutAssemble(binary, config);
    }
    std::string thread;
    std::function<void(const BinarySet&)> on_finalize;
};

class VectorMemIndexAsyncLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_enabled_ =
            segcore::storagev2translator::StorageV2AsyncLoadEnabled();
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(true);
        old_workers_ = storage::GetAsyncLoadThreadPoolSize();
        storage::SetAsyncLoadThreadPoolSize(1);
        old_slots_ =
            storage::LoadAdmissionController::GetInstance().CapacitySlots();
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(1);
        source_ = std::make_shared<VectorLoadSource>(temporary_.get().string());
    }
    void
    TearDown() override {
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const storage::LoadAdmissionRequest all{admission.CapacityBytes(), 1};
        const bool acquired =
            admission.TryAcquire(all, storage::LoadAdmissionPriority::High);
        EXPECT_TRUE(acquired);
        if (acquired) {
            admission.Release(all);
        }
        admission.SetCapacitySlots(old_slots_);
        storage::SetAsyncLoadThreadPoolSize(old_workers_);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
            old_enabled_);
    }
    storage::FileManagerContext
    Context() {
        return storage::FileManagerContext(storage::FieldDataMeta{1, 2, 3, 101},
                                           storage::IndexMeta{3, 101, 92001, 2},
                                           source_,
                                           nullptr);
    }
    std::string
    PersistBytes(const std::string& name, const uint8_t* data, size_t size) {
        const uint8_t empty = 0;
        storage::IndexData codec(size == 0 ? &empty : data, size);
        codec.SetFieldDataMeta(Context().fieldDataMeta);
        codec.set_index_meta(Context().indexMeta);
        auto encoded = codec.Serialize(storage::StorageType::Remote);
        const auto path = (temporary_.get() / name).string();
        source_->Write(path, encoded.data(), encoded.size());
        return path;
    }
    std::vector<std::string>
    Persist(BinarySet binary, bool sliced) {
        Assemble(binary);
        std::vector<std::string> files;
        files.reserve(binary.binary_map_.size() * 3 + 1);
        Config meta{{META, Config::array()}};
        for (const auto& [name, data] : binary.binary_map_) {
            if (sliced && data->size > 2) {
                // Unequal slices and reverse input order exercise placement.
                const auto part = data->size / 3;
                for (size_t i = 0; i < 3; ++i) {
                    const auto size = i == 2 ? data->size - 2 * part : part;
                    files.push_back(PersistBytes(GenSlicedFileName(name, i),
                                                 data->data.get() + i * part,
                                                 size));
                }
                meta[META].push_back(
                    {{NAME, name}, {SLICE_NUM, 3}, {TOTAL_LEN, data->size}});
            } else {
                files.push_back(
                    PersistBytes(name, data->data.get(), data->size));
            }
        }
        if (!meta[META].empty()) {
            const auto text = meta.dump();
            files.push_back(
                PersistBytes(INDEX_FILE_SLICE_META,
                             reinterpret_cast<const uint8_t*>(text.data()),
                             text.size()));
        }
        std::reverse(files.begin(), files.end());
        return files;
    }
    Config
    ConfigFor(const std::string& type = "FLAT",
              const std::string& metric = "L2",
              int64_t dim = 16) {
        return Config{{INDEX_TYPE, type},
                      {METRIC_TYPE, metric},
                      {DIM_KEY, std::to_string(dim)},
                      {"nlist", 4},
                      {"nprobe", 4},
                      {"M", 8},
                      {"efConstruction", 40},
                      {"ef", 40},
                      {LOAD_PRIORITY, proto::common::LoadPriority::LOW}};
    }
    template <typename T>
    void
    RoundTrip(const DatasetPtr& dataset,
              const Config& config,
              DataType elem_type = DataType::NONE,
              int32_t version =
                  knowhere::Version::GetCurrentVersion().VersionNumber()) {
        const auto type = config.at(INDEX_TYPE).get<std::string>();
        const auto metric = config.at(METRIC_TYPE).get<std::string>();
        ObservedVectorIndex<T> built(
            elem_type, type, metric, version, true, Context());
        built.BuildWithDataset(dataset, config);
        SearchInfo search;
        search.topk_ = 3;
        search.metric_type_ = metric;
        search.search_params_ = config;
        SearchResult expected;
        built.Query(dataset, search, nullptr, nullptr, expected);
        for (const bool sliced : {false, true}) {
            auto load_config = config;
            load_config[INDEX_FILES] = Persist(built.Serialize({}), sliced);
            for (const bool enabled : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << type << '/' << sliced << '/' << enabled);
                segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
                    enabled);
                source_->expect_async = enabled;
                ObservedVectorIndex<T> loaded(
                    elem_type, type, metric, version, true, Context());
                static_cast<IndexBase&>(loaded).Load({}, load_config, nullptr);
                EXPECT_EQ(loaded.thread.starts_with("MILVUS_ASYNC"), enabled);
                EXPECT_EQ(loaded.Count(), built.Count());
                EXPECT_EQ(loaded.GetDim(), built.GetDim());
                EXPECT_EQ(loaded.GetIdMap().OutCount(),
                          built.GetIdMap().OutCount());
                SearchResult actual;
                loaded.Query(dataset, search, nullptr, nullptr, actual);
                ASSERT_EQ(actual.seg_offsets_.size(),
                          expected.seg_offsets_.size());
                for (size_t i = 0; i < actual.seg_offsets_.size(); ++i) {
                    EXPECT_EQ(actual.seg_offsets_[i], expected.seg_offsets_[i]);
                    // Sparse queries can return fewer than topk hits; scores
                    // in the remaining invalid-ID slots are NaN placeholders.
                    if (expected.seg_offsets_[i] >= 0) {
                        EXPECT_FLOAT_EQ(actual.distances_[i],
                                        expected.distances_[i]);
                    }
                }
            }
        }
    }
    test::ScopedLoadTransientBudget budget_{1 << 20};
    test::TmpPath temporary_;
    std::shared_ptr<VectorLoadSource> source_;
    const int32_t version_ =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    bool old_enabled_{};
    int old_workers_{};
    size_t old_slots_{};
};

TEST_F(VectorMemIndexAsyncLoadTest, DenseFamiliesWithSlicedNullableData) {
    constexpr int rows = 400;
    std::vector<float> data(rows * 16);
    std::mt19937 engine(42);
    std::uniform_real_distribution<float> random(0.0, 1.0);
    std::generate(data.begin(), data.end(), [&] { return random(engine); });
    auto dataset = knowhere::GenDataSet(rows, 16, data.data());
    auto validity = test::MakeBoolArray(std::vector<bool>(rows + 2, true));
    validity[1] = false;
    validity[rows] = false;
    dataset->SetIdMapData(
        knowhere::IdMapData::FromValidData(validity.get(), rows + 2));
    for (const auto* type : {"FLAT", "IVF_FLAT", "IVF_SQ8", "HNSW"}) {
        RoundTrip<float>(dataset, ConfigFor(type));
    }
}

TEST_F(VectorMemIndexAsyncLoadTest, BinaryFamily) {
    std::vector<uint8_t> data(64 * 16);
    std::mt19937 engine(42);
    std::generate(data.begin(), data.end(), [&] { return engine(); });
    auto dataset = knowhere::GenDataSet(64, 128, data.data());
    RoundTrip<knowhere::bin1>(dataset, ConfigFor("BIN_FLAT", "HAMMING", 128));
}

TEST_F(VectorMemIndexAsyncLoadTest, SparseFamiliesRetainTheirInput) {
    std::vector<knowhere::sparse::SparseRow<float>> data;
    data.reserve(32);
    for (uint32_t i = 0; i < 32; ++i) {
        knowhere::sparse::SparseRow<float> row(2);
        row.set_at(0, i, 1.0F);
        row.set_at(1, 64 + i, 2.0F);
        data.push_back(std::move(row));
    }
    auto dataset = knowhere::GenDataSet(data.size(), 128, data.data());
    dataset->SetIsSparse(true);
    for (const auto* type : {"SPARSE_INVERTED_INDEX", "SPARSE_WAND"}) {
        for (const int32_t version : {6, version_}) {
            SCOPED_TRACE(version);
            RoundTrip<knowhere::sparse_u32_f32>(
                dataset, ConfigFor(type, "IP", 128), DataType::NONE, version);
        }
    }
}

TEST_F(VectorMemIndexAsyncLoadTest, AllNullAndEmptyEmbeddingLists) {
    for (const bool empty_lists : {false, true}) {
        const auto metric =
            empty_lists ? knowhere::metric::MAX_SIM : knowhere::metric::L2;
        const auto element =
            empty_lists ? DataType::VECTOR_FLOAT : DataType::NONE;
        auto config = ConfigFor("HNSW", metric);
        const bool valid[]{false, empty_lists, empty_lists};
        auto dataset = knowhere::GenDataSet(0, 16, nullptr);
        dataset->SetIdMapData(knowhere::IdMapData::FromValidData(valid, 3));
        ObservedVectorIndex<float> built(
            element, "HNSW", metric, version_, true, Context());
        built.BuildWithDataset(dataset, config);
        config[INDEX_FILES] = Persist(built.Serialize({}), true);
        source_->expect_async = true;
        ObservedVectorIndex<float> loaded(
            element, "HNSW", metric, version_, true, Context());
        loaded.Load({}, config, nullptr);
        EXPECT_EQ(loaded.Count(), 0);
        EXPECT_EQ(loaded.GetDim(), 16);
        EXPECT_EQ(loaded.GetIdMap().OutCount(), 3);
        EXPECT_EQ(loaded.GetValidCount(), empty_lists ? 2 : 0);
        if (empty_lists) {
            int64_t ids[]{1, 2};
            const auto [data, offsets] =
                loaded.GetEmbListByIds(GenIdsDataset(2, ids), metric);
            EXPECT_TRUE(data.empty());
            EXPECT_EQ(offsets, std::vector<size_t>({0, 0, 0}));
        }
    }
}

TEST_F(VectorMemIndexAsyncLoadTest, EmbeddingListMetadataAndRawIndexSidecars) {
    std::vector<float> data(32 * 16);
    std::iota(data.begin(), data.end(), 0.1F);
    std::vector<size_t> offsets(17);
    for (size_t i = 0; i < offsets.size(); ++i) {
        offsets[i] = 2 * i;
    }
    auto dataset = knowhere::GenDataSet(32, 16, data.data());
    dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                 static_cast<const size_t*>(offsets.data()));
    dataset->Set(knowhere::meta::EMB_LIST_COUNT, int64_t{16});
    const auto metric = knowhere::metric::MAX_SIM_COSINE;
    for (const auto* strategy : {"tokenann", "muvera"}) {
        SCOPED_TRACE(strategy);
        const auto version =
            std::string_view(strategy) == "muvera"
                ? knowhere::Version::GetMaximumVersion().VersionNumber()
                : version_;
        auto config = ConfigFor("HNSW", metric);
        config["emb_list_strategy"] = strategy;
        config["muvera_num_projections"] = 2;
        config["muvera_num_repeats"] = 2;
        ObservedVectorIndex<float> built(
            DataType::VECTOR_FLOAT, "HNSW", metric, version, true, Context());
        built.BuildWithDataset(dataset, config);
        auto binary = built.Serialize({});
        Assemble(binary);
        ASSERT_TRUE(binary.Contains(knowhere::meta::EMB_LIST_META));
        if (std::string_view(strategy) == "muvera") {
            ASSERT_TRUE(binary.Contains(knowhere::meta::EMB_LIST_RAW_INDEX));
        }
        int64_t ids[]{1, 3, 7};
        const auto ids_dataset = GenIdsDataset(3, ids);
        const auto expected = built.GetEmbListByIds(ids_dataset, metric);
        config[INDEX_FILES] = Persist(std::move(binary), true);
        source_->expect_async = true;
        ObservedVectorIndex<float> loaded(
            DataType::VECTOR_FLOAT, "HNSW", metric, version, true, Context());
        loaded.Load({}, config, nullptr);
        const auto actual = loaded.GetEmbListByIds(ids_dataset, metric);
        EXPECT_EQ(actual, expected);
    }
}

TEST_F(VectorMemIndexAsyncLoadTest, CancellationAndReadFailuresDoNotFinalize) {
    const std::vector<uint8_t> bytes(1024, 42);
    BinarySet binary;
    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[bytes.size()]);
    std::copy(bytes.begin(), bytes.end(), data.get());
    binary.Append("FLAT", data, bytes.size());
    auto config = ConfigFor();
    config[INDEX_FILES] = Persist(binary, true);
    for (const int mode : {0, 1, 2}) {
        SCOPED_TRACE(mode);
        folly::CancellationSource cancel;
        OpContext op;
        op.cancellation_token = cancel.getToken();
        if (mode == 0) {
            cancel.requestCancellation();
        }
        source_->expect_async = true;
        source_->on_read = [&] {
            if (mode == 1) {
                cancel.requestCancellation();
            } else if (mode == 2) {
                ThrowInfo(FileReadFailed, "injected read failure");
            }
        };
        ObservedVectorIndex<float> loaded(
            DataType::NONE, "FLAT", "L2", version_, true, Context());
        try {
            loaded.Load({}, config, &op);
            FAIL() << "load must fail";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(),
                      mode == 2 ? FileReadFailed : FollyCancel);
        }
        EXPECT_TRUE(loaded.thread.empty());
    }
}

TEST_F(VectorMemIndexAsyncLoadTest, NativeReadSuspendsWorkerAndDrainsOnCancel) {
    std::vector<float> data(64 * 16, 0.5F);
    auto config = ConfigFor();
    ObservedVectorIndex<float> built(
        DataType::NONE, "FLAT", "L2", version_, true, Context());
    built.BuildWithDataset(knowhere::GenDataSet(64, 16, data.data()), config);
    const auto files = Persist(built.Serialize({}), true);
    config[INDEX_FILES] = files;
    for (const bool cancelled : {false, true}) {
        auto fs = std::make_shared<VectorNativeFileSystem>();
        for (const auto& path : files) {
            std::vector<uint8_t> content(source_->Size(path));
            source_->Read(path, content.data(), content.size());
            fs->files.emplace(path,
                              std::make_shared<test::ControlledDirectReadFile>(
                                  std::move(content)));
        }
        const auto first = fs->files.at(files.front());
        first->SetAutoComplete(false);
        auto context = Context();
        context.fs = fs;
        ObservedVectorIndex<float> loaded(
            DataType::NONE, "FLAT", "L2", version_, true, context);
        folly::CancellationSource cancel;
        OpContext op;
        op.cancellation_token = cancel.getToken();
        auto task = std::async(std::launch::async,
                               [&] { loaded.Load({}, config, &op); });
        auto cleanup = folly::makeGuard([&] {
            first->SetAutoComplete(true);
            if (first->WaitForCallCount(1)) {
                first->Complete(0);
            }
            task.wait();
        });
        ASSERT_TRUE(first->WaitForCallCount(1));
        auto worker_free = std::make_shared<std::promise<void>>();
        auto probe = worker_free->get_future();
        storage::ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH)
            ->add([worker_free] { worker_free->set_value(); });
        ASSERT_EQ(probe.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        if (cancelled) {
            cancel.requestCancellation();
        }
        EXPECT_EQ(task.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool acquired =
            admission.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High);
        EXPECT_FALSE(acquired);
        if (acquired) {
            admission.Release({0, 1});
        }
        first->SetAutoComplete(true);
        first->Complete(0);
        task.wait();
        cleanup.dismiss();
        if (cancelled) {
            EXPECT_THROW(task.get(), SegcoreError);
            EXPECT_TRUE(loaded.thread.empty());
        } else {
            EXPECT_NO_THROW(task.get());
            EXPECT_EQ(loaded.Count(), 64);
            EXPECT_TRUE(loaded.thread.starts_with("MILVUS_ASYNC"));
        }
        for (const auto& [path, file] : fs->files) {
            EXPECT_EQ(file->ReadAtCalls(), 0);
            EXPECT_EQ(file->AsyncReadCalls(), 0);
            EXPECT_LE(file->PeakInflight(), 1);
        }
    }
}

TEST_F(VectorMemIndexAsyncLoadTest, CancelWaitingForAdmission) {
    const uint8_t byte = 1;
    auto config = ConfigFor();
    config[INDEX_FILES] =
        std::vector<std::string>{PersistBytes("FLAT", &byte, 1)};
    auto& admission = storage::LoadAdmissionController::GetInstance();
    ASSERT_TRUE(
        admission.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High));
    const auto release = folly::makeGuard([&] { admission.Release({0, 1}); });
    folly::CancellationSource cancel;
    OpContext op;
    op.cancellation_token = cancel.getToken();
    ObservedVectorIndex<float> loaded(
        DataType::NONE, "FLAT", "L2", version_, true, Context());
    auto task =
        std::async(std::launch::async, [&] { loaded.Load({}, config, &op); });
    EXPECT_EQ(task.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    cancel.requestCancellation();
    EXPECT_THROW(task.get(), SegcoreError);
    EXPECT_EQ(source_->reads, 0);
    EXPECT_TRUE(loaded.thread.empty());
}

TEST_F(VectorMemIndexAsyncLoadTest, FinalizationFailureReleasesBinarySet) {
    const uint8_t byte = 1;
    auto config = ConfigFor();
    config[INDEX_FILES] =
        std::vector<std::string>{PersistBytes("FLAT", &byte, 1)};
    ObservedVectorIndex<float> loaded(
        DataType::NONE, "FLAT", "L2", version_, true, Context());
    std::weak_ptr<uint8_t[]> input;
    loaded.on_finalize = [&](const BinarySet& binary) {
        input = binary.GetByName("FLAT")->data;
        throw std::bad_alloc();
    };
    try {
        loaded.Load({}, config, nullptr);
        FAIL() << "finalization must fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), MemAllocateFailed);
    }
    EXPECT_TRUE(input.expired());
}

TEST_F(VectorMemIndexAsyncLoadTest, CancellationDrainsSynchronousFinalization) {
    std::vector<float> data(64 * 16, 0.5F);
    auto config = ConfigFor();
    ObservedVectorIndex<float> built(
        DataType::NONE, "FLAT", "L2", version_, true, Context());
    built.BuildWithDataset(knowhere::GenDataSet(64, 16, data.data()), config);
    config[INDEX_FILES] = Persist(built.Serialize({}), true);
    ObservedVectorIndex<float> loaded(
        DataType::NONE, "FLAT", "L2", version_, true, Context());
    folly::CancellationSource cancel;
    OpContext op;
    op.cancellation_token = cancel.getToken();
    std::promise<void> entered;
    auto started = entered.get_future();
    std::promise<void> release;
    auto finish = release.get_future();
    std::weak_ptr<uint8_t[]> input;
    loaded.on_finalize = [&](const BinarySet& binary) {
        input = binary.GetByName("FLAT")->data;
        entered.set_value();
        finish.wait();
    };
    auto task =
        std::async(std::launch::async, [&] { loaded.Load({}, config, &op); });
    auto cleanup = folly::makeGuard([&] {
        release.set_value();
        task.wait();
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    cancel.requestCancellation();
    EXPECT_EQ(task.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    EXPECT_FALSE(input.expired());
    cleanup.dismiss();
    release.set_value();
    try {
        task.get();
        FAIL() << "cancellation must be reported after finalization drains";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FollyCancel);
    }
    EXPECT_TRUE(input.expired());
}

TEST_F(VectorMemIndexAsyncLoadTest,
       MemoryEstimateIncludesDecodedInputAndScratch) {
    std::vector<float> data(64 * 16, 0.5F);
    auto config = ConfigFor();
    ObservedVectorIndex<float> built(
        DataType::NONE, "FLAT", "L2", version_, true, Context());
    built.BuildWithDataset(knowhere::GenDataSet(64, 16, data.data()), config);
    const auto files = Persist(built.Serialize({}), true);
    uint64_t retained = 0;
    uint64_t scratch = 0;
    for (const auto& file : files) {
        auto input = storage::OpenLegacyIndexInput(source_, nullptr, file);
        const auto info =
            folly::coro::blockingWait(storage::InspectLegacyIndexFileAsync(
                *input, proto::common::LoadPriority::HIGH));
        retained += info.payload_bytes;
        scratch = std::max(scratch, uint64_t{info.max_transient_bytes});
    }
    for (const bool enabled : {false, true}) {
        source_->expect_async = enabled;
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(enabled);
        const auto request = IndexFactory::GetInstance().IndexLoadResource(
            DataType::VECTOR_FLOAT,
            DataType::NONE,
            version_,
            1,
            {{INDEX_TYPE, "FLAT"}, {METRIC_TYPE, "L2"}},
            false,
            64,
            16,
            files,
            Context(),
            nullptr,
            nullptr);
        EXPECT_GE(request.max_memory_cost,
                  request.final_memory_cost + retained + scratch);
    }
}

}  // namespace
}  // namespace milvus::index
