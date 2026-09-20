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

#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/IndexEntryTarget.h"
#include "storage/IndexEntryWriter.h"
#include "storage/IndexEntryReader.h"
#include "folly/system/ThreadName.h"
#include "folly/ScopeGuard.h"
#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <arrow/filesystem/localfs.h>
#include <folly/FBVector.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/CDataType.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "gtest/gtest.h"
#include "index/BitmapIndex.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/IndexLoadUtils.h"
#include "index/JsonIndexLoadPlan.h"
#include "index/NgramInvertedIndex.h"
#include "folly/coro/BlockingWait.h"
#include "index/InvertedIndexTantivy.h"
#include "index/JsonFlatIndex.h"
#include "index/ScalarIndex.h"
#include "index/ScalarIndexSort.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/LocalFileIOPool.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "index/StringIndexMarisa.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/schema.pb.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"

constexpr int64_t nb = 100;
namespace indexcgo = milvus::proto::indexcgo;
namespace schemapb = milvus::proto::schema;
using milvus::index::ScalarIndexPtr;
using milvus::segcore::GeneratedData;
template <typename T>
class TypedScalarIndexTest : public ::testing::Test {
 protected:
    // void
    // SetUp() override {
    // }

    // void
    // TearDown() override {
    // }
};

TYPED_TEST_SUITE_P(TypedScalarIndexTest);

TYPED_TEST_P(TypedScalarIndexTest, Dummy) {
    using T = TypeParam;
    std::cout << typeid(T()).name() << std::endl;
    std::cout << milvus::GetDType<T>() << std::endl;
}

auto
GetTempFileManagerCtx(CDataType data_type) {
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(
        static_cast<milvus::proto::schema::DataType>(data_type));
    milvus::storage::IndexMeta index_meta{3, 101, 1000, 10000};
    auto ctx = milvus::storage::FileManagerContext(
        field_meta, index_meta, chunk_manager, fs);
    return ctx;
}

TEST(LegacyHybridResourceEstimate, ResolvesPersistedChildType) {
    using namespace milvus;
    using namespace milvus::index;
    auto ctx = GetTempFileManagerCtx(Int64);
    ctx.indexMeta.build_id = 5348301;
    storage::MemFileManagerImpl manager(ctx);
    auto cleanup = folly::makeGuard([&] {
        for (const auto& [path, size] : manager.GetRemotePathsToFileSize()) {
            ctx.chunkManagerPtr->Remove(path);
        }
    });
    constexpr uint64_t index_size = 1UL << 20;
    constexpr int64_t rows = 10001;
    for (const auto& [type, name] :
         {std::pair{ScalarIndexType::BITMAP, BITMAP_INDEX_TYPE},
          std::pair{ScalarIndexType::INVERTED, INVERTED_INDEX_TYPE},
          std::pair{ScalarIndexType::STLSORT, ASCENDING_SORT},
          std::pair{ScalarIndexType::MARISA, MARISA_TRIE}}) {
        // Use the same standalone binlog encoding as legacy HYBRID uploads.
        BinarySet binary_set;
        auto data = std::make_shared<uint8_t[]>(1);
        data[0] = static_cast<uint8_t>(type);
        binary_set.Append(INDEX_TYPE, data, 1);
        ASSERT_TRUE(manager.AddFile(binary_set));
        ASSERT_EQ(manager.GetRemotePathsToFileSize().size(), 1);
        const std::vector<std::string> files{
            TestLocalPath + "/unread_index_payload",
            manager.GetRemotePathsToFileSize().begin()->first};
        for (const auto* version : {"1", "2", ""}) {
            std::map<std::string, std::string> params{
                {INDEX_TYPE, HYBRID_INDEX_TYPE}};
            if (*version != '\0') {
                params[SCALAR_INDEX_ENGINE_VERSION] = version;
            }
            auto child_params = params;
            child_params[INDEX_TYPE] = name;
            for (bool async : {false, true}) {
                ctx.use_async_load = async;
                for (bool mmap : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << name << " version=" << version
                                 << " async=" << async << " mmap=" << mmap);
                    const auto expected =
                        IndexFactory::GetInstance().ScalarIndexLoadResource(
                            DataType::INT64,
                            0,
                            index_size,
                            child_params,
                            mmap,
                            rows);
                    const auto resources =
                        IndexFactory::GetInstance().ScalarIndexFileLoadResource(
                            DataType::INT64,
                            index_size,
                            params,
                            mmap,
                            rows,
                            files,
                            ctx);
                    EXPECT_EQ(resources.request.final_memory_cost,
                              expected.final_memory_cost);
                    EXPECT_EQ(resources.request.final_disk_cost,
                              expected.final_disk_cost);
                    EXPECT_EQ(resources.request.max_memory_cost,
                              expected.max_memory_cost);
                    EXPECT_EQ(resources.request.max_disk_cost,
                              expected.max_disk_cost);
                    EXPECT_EQ(resources.request.has_raw_data,
                              expected.has_raw_data);
                    EXPECT_FALSE(resources.overhead.has_value());
                }
            }
        }
    }
}

TEST(LegacyHybridResourceEstimate, FallsBackWhenChildTypeIsUnavailable) {
    using namespace milvus;
    using namespace milvus::index;
    auto ctx = GetTempFileManagerCtx(Int64);
    ctx.indexMeta.build_id = 5348302;
    storage::MemFileManagerImpl manager(ctx);
    auto cleanup = folly::makeGuard([&] {
        for (const auto& [path, size] : manager.GetRemotePathsToFileSize()) {
            ctx.chunkManagerPtr->Remove(path);
        }
    });
    constexpr uint64_t index_size = 1UL << 20;
    const std::map<std::string, std::string> params{
        {INDEX_TYPE, HYBRID_INDEX_TYPE}};
    auto check_fallback = [&](const std::vector<std::string>& files,
                              const storage::FileManagerContext& context) {
        const auto resources =
            IndexFactory::GetInstance().ScalarIndexFileLoadResource(
                DataType::INT64,
                index_size,
                params,
                false,
                10001,
                files,
                context);
        EXPECT_EQ(resources.request.final_memory_cost, index_size);
        EXPECT_EQ(resources.request.final_disk_cost, index_size);
        EXPECT_EQ(resources.request.max_memory_cost, 2 * index_size);
        EXPECT_EQ(resources.request.max_disk_cost, index_size);
        EXPECT_FALSE(resources.request.has_raw_data);
        EXPECT_FALSE(resources.overhead.has_value());
    };
    check_fallback({}, ctx);
    check_fallback({TestLocalPath + "/missing_legacy_hybrid/index_type"}, ctx);
    // Unknown type and invalid payload length must not be decoded as a child.
    for (const size_t size : {1, 2}) {
        BinarySet binary_set;
        auto data = std::make_shared<uint8_t[]>(size);
        data[0] =
            size == 1 ? 255 : static_cast<uint8_t>(ScalarIndexType::BITMAP);
        binary_set.Append(INDEX_TYPE, data, size);
        ASSERT_TRUE(manager.AddFile(binary_set));
        const auto path = manager.GetRemotePathsToFileSize().begin()->first;
        check_fallback({path}, ctx);
        check_fallback({path}, storage::FileManagerContext{});
    }
}

class TestScalarIndexV3LoadRoute : public milvus::index::ScalarIndex<int32_t> {
 public:
    struct CleanupThreadRecorder {
        explicit CleanupThreadRecorder(std::string& thread) : thread_(thread) {
        }
        ~CleanupThreadRecorder() {
            thread_ = folly::getCurrentThreadName().value_or("");
        }
        std::string& thread_;
    };

    explicit TestScalarIndexV3LoadRoute(
        const milvus::storage::FileManagerContext& ctx)
        : milvus::index::ScalarIndex<int32_t>("test_scalar_v3_async") {
        file_manager_ =
            std::make_shared<milvus::storage::MemFileManagerImpl>(ctx);
    }

    milvus::index::ScalarIndexType
    GetIndexType() const override {
        return milvus::index::ScalarIndexType::STLSORT;
    }

    knowhere::BinarySet
    Serialize(const milvus::Config&) override {
        return {};
    }

    void
    Load(const knowhere::BinarySet&, const milvus::Config&) override {
    }

    void
    Load(milvus::tracer::TraceContext, const milvus::Config&) override {
    }

    void
    Build(const milvus::Config&) override {
    }

    void
    Build(size_t, const int32_t*, const bool* = nullptr) override {
    }

    int64_t
    Count() override {
        return load_entries_calls_;
    }

    milvus::index::IndexStatsPtr
    Upload(const milvus::Config& config) override {
        return UploadUnified(config);
    }

    const bool
    HasRawData() const override {
        return false;
    }

    const milvus::TargetBitmap
    In(size_t, const int32_t*) override {
        return {};
    }

    const milvus::TargetBitmap
    IsNull() override {
        return {};
    }

    milvus::TargetBitmap
    IsNotNull() override {
        return {};
    }

    const milvus::TargetBitmap
    NotIn(size_t, const int32_t*) override {
        return {};
    }

    const milvus::TargetBitmap
    Range(const int32_t&, milvus::OpType) override {
        return {};
    }

    const milvus::TargetBitmap
    Range(const int32_t&, bool, const int32_t&, bool) override {
        return {};
    }

    std::optional<int32_t>
    Reverse_Lookup(size_t) const override {
        return std::nullopt;
    }

    int64_t
    Size() override {
        return 0;
    }

    void
    WriteEntries(milvus::storage::IndexEntryWriter* writer) override {
        constexpr int32_t payload = 42;
        writer->WriteEntry("payload", &payload, sizeof(payload));
    }

    void
    LoadEntries(milvus::storage::IndexEntryReader& reader,
                const milvus::Config&) override {
        auto entry = reader.ReadEntry("payload");
        ASSERT_EQ(entry.data.size(), sizeof(int32_t));
        load_entries_calls_++;
    }

    milvus::index::IndexLoadPlan
    PlanLoad(const milvus::storage::IndexEntryDirectory& directory,
             const nlohmann::json& metadata,
             const milvus::Config&) override {
        planned_thread_ = folly::getCurrentThreadName().value_or("");
        const auto bytes = directory.At("payload").plaintext_size;
        milvus::index::IndexLoadPlan plan;
        plan.load_context =
            std::make_shared<CleanupThreadRecorder>(cleanup_thread_);
        milvus::storage::EntryTarget target;
        if (mmap_target_path_.empty()) {
            auto payload = std::make_shared<std::vector<uint8_t>>(bytes);
            target = milvus::storage::MemoryEntryTarget{
                payload, payload->data(), payload->size()};
        } else {
            auto staging = std::make_shared<milvus::storage::IndexFileTarget>(
                mmap_target_path_, bytes, retain_file_);
            target = milvus::storage::FileEntryTarget{staging, 0, bytes};
        }
        plan.entries.push_back(milvus::storage::EntryLoadPlan{
            fail_read_ ? "missing" : "payload", std::move(target)});
        return plan;
    }

    folly::coro::Task<void>
    FinishLoadAsync(milvus::index::IndexLoadPlan& plan,
                    const milvus::Config&) override {
        finish_load_thread_ = folly::getCurrentThreadName().value_or("");
        finish_load_calls_++;
        if (fail_finish_load_) {
            ThrowInfo(milvus::ErrorCode::FileWriteFailed,
                      "injected scalar finish-load failure");
        }
        const auto& target = plan.At("payload").target;
        if (const auto* memory =
                std::get_if<milvus::storage::MemoryEntryTarget>(&target)) {
            EXPECT_EQ(memory->bytes, sizeof(int32_t));
            std::memcpy(&loaded_payload_, memory->data, sizeof(int32_t));
        } else {
            const auto& mmap =
                std::get<milvus::storage::FileEntryTarget>(target);
            EXPECT_THROW(mmap.staging->WriteAt(0, nullptr, 0),
                         milvus::SegcoreError);
            std::ifstream file(mmap.staging->path, std::ios::binary);
            file.read(reinterpret_cast<char*>(&loaded_payload_),
                      sizeof(loaded_payload_));
            EXPECT_TRUE(file.good());
        }
        if (cancel_on_finish_) {
            cancel_on_finish_->requestCancellation();
        }
        co_return;
    }

    folly::CancellationSource* cancel_on_finish_{nullptr};
    bool retain_file_{false};
    std::string planned_thread_;
    std::string finish_load_thread_;
    std::string cleanup_thread_;
    std::string mmap_target_path_;
    bool fail_read_{false};
    bool fail_finish_load_{false};
    int load_entries_calls_{0};
    int finish_load_calls_{0};
    int32_t loaded_payload_{0};
};

namespace {
class RecordingOpenFileSystem : public arrow::fs::LocalFileSystem {
 public:
    explicit RecordingOpenFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs)
        : fs_(std::move(fs)) {
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ++sync_opens;
        return fs_->OpenInputFile(path);
    }

    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        ++async_opens;
        if (cancel_on_open) {
            cancel_on_open->requestCancellation();
            return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
                MakeFinished(arrow::Status::IOError(
                    "injected open failure after cancellation"));
        }
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(fs_->OpenInputFile(path));
    }

    folly::CancellationSource* cancel_on_open{nullptr};
    int sync_opens{0};
    int async_opens{0};

 private:
    std::shared_ptr<arrow::fs::FileSystem> fs_;
};
}  // namespace

TEST(ScalarIndexV3AsyncLoadConfigTest,
     ResourceInspectionSelectsStreamOpenMode) {
    using namespace milvus;
    using namespace milvus::index;
    auto ctx = GetTempFileManagerCtx(Int32);
    ScalarIndexSort<int32_t> built(ctx);
    const std::vector<int32_t> values{30, 10, 20, 10};
    built.Build(values.size(), values.data());
    const auto stats = built.UploadUnified({});
    const auto fs = std::make_shared<RecordingOpenFileSystem>(ctx.fs);
    ctx.fs = fs;
    const std::map<std::string, std::string> params{
        {INDEX_TYPE, ASCENDING_SORT}, {SCALAR_INDEX_ENGINE_VERSION, "3"}};
    for (bool async : {true, false}) {
        ctx.use_async_load = async;
        fs->sync_opens = fs->async_opens = 0;
        (void)IndexFactory::GetInstance().ScalarIndexFileLoadResource(
            DataType::INT32,
            stats->GetSerializedSize(),
            params,
            false,
            values.size(),
            stats->GetIndexFiles(),
            ctx);
        EXPECT_EQ(fs->sync_opens, async ? 0 : 1);
        EXPECT_EQ(fs->async_opens, async ? 1 : 0);
    }
}

TEST(ScalarIndexV3AsyncLoadConfigTest, CancellationWinsOverOpenFailure) {
    using namespace milvus;
    using namespace milvus::index;
    auto ctx = GetTempFileManagerCtx(Int32);
    TestScalarIndexV3LoadRoute built(ctx);
    auto stats = built.UploadUnified({});
    auto fs = std::make_shared<RecordingOpenFileSystem>(ctx.fs);
    folly::CancellationSource cancelled;
    fs->cancel_on_open = &cancelled;
    ctx.fs = fs;
    ctx.use_async_load = true;
    TestScalarIndexV3LoadRoute loaded(ctx);
    Config config;
    config[INDEX_FILES] = stats->GetIndexFiles();
    OpContext op_ctx;
    op_ctx.cancellation_token = cancelled.getToken();
    try {
        loaded.LoadUnified(config, &op_ctx);
        FAIL() << "Expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_GT(fs->async_opens, 0);
    EXPECT_EQ(loaded.finish_load_calls_, 0);
}

TEST(ScalarIndexV3AsyncLoadConfigTest,
     CancellationDuringFinalizationPreventsCommit) {
    using namespace milvus;
    using namespace milvus::index;
    auto ctx = GetTempFileManagerCtx(Int32);
    ctx.use_async_load = true;
    TestScalarIndexV3LoadRoute built(ctx);
    auto stats = built.UploadUnified({});
    Config config;
    config[INDEX_FILES] = stats->GetIndexFiles();
    for (bool file : {false, true}) {
        folly::CancellationSource cancelled;
        TestScalarIndexV3LoadRoute loaded(ctx);
        loaded.cancel_on_finish_ = &cancelled;
        loaded.retain_file_ = true;
        if (file) {
            loaded.mmap_target_path_ = TestLocalPath + "/cancel_finish/payload";
        }
        OpContext op_ctx;
        op_ctx.cancellation_token = cancelled.getToken();
        try {
            loaded.LoadUnified(config, &op_ctx);
            FAIL() << "Expected cancellation";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
        }
        EXPECT_EQ(loaded.finish_load_calls_, 1);
        if (file) {
            EXPECT_FALSE(std::filesystem::exists(loaded.mmap_target_path_));
            EXPECT_TRUE(loaded.cleanup_thread_.starts_with("MILVUS_LF_IO_") ||
                        loaded.cleanup_thread_.starts_with("MILVUS_ASYNC"));
        }
    }
}

TEST(ScalarIndexV3AsyncLoadConfigTest, GlobalSwitchSelectsCompleteLoadPath) {
    using namespace milvus;
    using namespace milvus::index;
    using namespace milvus::segcore::storagev2translator;
    const auto old_enabled = StorageV2AsyncLoadEnabled();
    auto restore = folly::makeGuard(
        [old_enabled] { SetStorageV2AsyncLoadEnabled(old_enabled); });
    for (bool enabled : {false, true}) {
        SetStorageV2AsyncLoadEnabled(enabled);
        for (auto priority : {proto::common::LoadPriority::HIGH,
                              proto::common::LoadPriority::LOW}) {
            auto ctx = GetTempFileManagerCtx(Int32);
            TestScalarIndexV3LoadRoute build_index(ctx);
            auto stats = build_index.UploadUnified({});
            TestScalarIndexV3LoadRoute load_index(ctx);
            Config config;
            config[INDEX_FILES] = stats->GetIndexFiles();
            config[LOAD_PRIORITY] = priority;
            OpContext op_ctx;
            load_index.LoadUnified(config, &op_ctx);
            EXPECT_EQ(load_index.load_entries_calls_, enabled ? 0 : 1);
            EXPECT_EQ(load_index.finish_load_calls_, enabled ? 1 : 0);
            if (enabled) {
                EXPECT_EQ(load_index.loaded_payload_, 42);
                EXPECT_TRUE(
                    load_index.planned_thread_.starts_with("MILVUS_ASYNC"));
                EXPECT_TRUE(
                    load_index.finish_load_thread_.starts_with("MILVUS_ASYNC"));
            }
        }
    }
}

TEST(ScalarIndexV3AsyncLoadConfigTest,
     FileTargetsFinishLoadOnAsyncAndCleanUpOnLocalFileIOPool) {
    using namespace milvus;
    using namespace milvus::index;
    using namespace milvus::segcore::storagev2translator;
    const auto previous = StorageV2AsyncLoadEnabled();
    auto& pool = storage::LocalFileIOPool::GetInstance();
    auto restore = folly::makeGuard([&] {
        SetStorageV2AsyncLoadEnabled(previous);
        pool.Configure(0);
    });
    SetStorageV2AsyncLoadEnabled(true);
    auto ctx = GetTempFileManagerCtx(Int32);
    TestScalarIndexV3LoadRoute build_index(ctx);
    auto stats = build_index.UploadUnified({});
    enum class Outcome { Success, ReadFailure, FinishLoadFailure };
    for (int workers : {0, 1}) {
        pool.Configure(workers);
        for (auto priority : {proto::common::LoadPriority::HIGH,
                              proto::common::LoadPriority::LOW}) {
            for (bool file_target : {false, true}) {
                for (auto outcome : {Outcome::Success,
                                     Outcome::ReadFailure,
                                     Outcome::FinishLoadFailure}) {
                    SCOPED_TRACE(::testing::Message()
                                 << workers << "/" << priority << "/"
                                 << file_target << "/"
                                 << static_cast<int>(outcome));
                    TestScalarIndexV3LoadRoute loaded(ctx);
                    if (file_target) {
                        loaded.mmap_target_path_ =
                            TestLocalPath + "/scalar_route_staging/payload";
                    }
                    loaded.fail_read_ = outcome == Outcome::ReadFailure;
                    loaded.fail_finish_load_ =
                        outcome == Outcome::FinishLoadFailure;
                    Config config;
                    config[INDEX_FILES] = stats->GetIndexFiles();
                    config[LOAD_PRIORITY] = priority;
                    // File-backed construction also occurs without final mmap.
                    config[ENABLE_MMAP] = false;
                    if (outcome != Outcome::Success) {
                        try {
                            loaded.LoadUnified(config);
                            FAIL() << "expected read or finish-load failure";
                        } catch (const SegcoreError& error) {
                            if (outcome == Outcome::FinishLoadFailure) {
                                EXPECT_EQ(error.get_error_code(),
                                          ErrorCode::FileWriteFailed);
                            } else {
                                EXPECT_NE(
                                    std::string(error.what()).find("missing"),
                                    std::string::npos);
                            }
                        }
                    } else {
                        loaded.LoadUnified(config);
                        EXPECT_EQ(loaded.loaded_payload_, 42);
                    }
                    EXPECT_EQ(loaded.load_entries_calls_, 0);
                    EXPECT_EQ(loaded.finish_load_calls_,
                              outcome == Outcome::ReadFailure ? 0 : 1);
                    EXPECT_TRUE(
                        loaded.planned_thread_.starts_with("MILVUS_ASYNC"));
                    const auto prefix = workers > 0 && file_target
                                            ? "MILVUS_LF_IO_"
                                            : "MILVUS_ASYNC";
                    if (outcome != Outcome::ReadFailure) {
                        EXPECT_TRUE(loaded.finish_load_thread_.starts_with(
                            "MILVUS_ASYNC"));
                    }
                    EXPECT_TRUE(loaded.cleanup_thread_.starts_with(prefix));
                    if (file_target) {
                        EXPECT_FALSE(
                            std::filesystem::exists(loaded.mmap_target_path_));
                    }
                }
            }
        }
    }
}

TEST(ScalarIndexV3AsyncLoadConfigTest, SortMmapLoadWithSingleLocalFileWorker) {
    using namespace milvus;
    using namespace milvus::index;
    using namespace milvus::segcore::storagev2translator;
    const auto previous = StorageV2AsyncLoadEnabled();
    auto& pool = storage::LocalFileIOPool::GetInstance();
    auto restore = folly::makeGuard([&] {
        SetStorageV2AsyncLoadEnabled(previous);
        pool.Configure(0);
    });
    SetStorageV2AsyncLoadEnabled(true);
    pool.Configure(1);
    auto ctx = GetTempFileManagerCtx(Int32);
    ScalarIndexSort<int32_t> built(ctx);
    const std::vector<int32_t> values{30, 10, 20, 10};
    built.Build(values.size(), values.data());
    const auto stats = built.UploadUnified({});
    Config config;
    config[INDEX_FILES] = stats->GetIndexFiles();
    config[ENABLE_MMAP] = true;
    config[LOAD_PRIORITY] = proto::common::LoadPriority::LOW;
    ScalarIndexSort<int32_t> loaded(ctx);
    loaded.LoadUnified(config);
    EXPECT_EQ(loaded.Count(), values.size());
    const int32_t needle = 10;
    auto hits = loaded.In(1, &needle);
    ASSERT_EQ(hits.size(), values.size());
    EXPECT_EQ(hits.count(), 2);
    EXPECT_TRUE(hits[1]);
    EXPECT_TRUE(hits[3]);
}

TEST(ScalarIndexV3AsyncLoadConfigTest,
     HybridLoadsStandaloneSortWithoutTypeMeta) {
    using namespace milvus;
    using namespace milvus::index;
    using namespace milvus::segcore::storagev2translator;
    const auto previous = StorageV2AsyncLoadEnabled();
    auto restore = folly::makeGuard(
        [previous] { SetStorageV2AsyncLoadEnabled(previous); });
    auto ctx = GetTempFileManagerCtx(Int64);
    ScalarIndexSort<int64_t> build_index(ctx);
    const std::vector<int64_t> values{30, 10, 20, 10};
    build_index.Build(values.size(), values.data());
    auto stats = build_index.UploadUnified({});
    Config config;
    config[INDEX_FILES] = stats->GetIndexFiles();
    config[LOAD_PRIORITY] = proto::common::LoadPriority::LOW;
    for (bool enabled : {false, true}) {
        SetStorageV2AsyncLoadEnabled(enabled);
        HybridScalarIndex<int64_t> loaded(7, ctx);
        loaded.LoadUnified(config);
        EXPECT_EQ(loaded.Count(), values.size());
        const int64_t needle = 10;
        auto hits = loaded.In(1, &needle);
        ASSERT_EQ(hits.size(), values.size());
        EXPECT_EQ(hits.count(), 2);
        EXPECT_TRUE(hits[1]);
        EXPECT_TRUE(hits[3]);
        EXPECT_EQ(loaded.IsNull().count(), 0);
        EXPECT_EQ(loaded.IsNotNull().count(), values.size());
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Constructor) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Count) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        ASSERT_EQ(nb, scalar_index->Count());
    }
}

TYPED_TEST_P(TypedScalarIndexTest, HasRawData) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        ASSERT_EQ(nb, scalar_index->Count());
        ASSERT_TRUE(scalar_index->HasRawData());
    }
}

TEST(ScalarIndexPlannerPolicy, PatternOpsRequireString) {
    milvus::index::BitmapIndex<int64_t> int_index;
    EXPECT_FALSE(int_index.ShouldUseOp(milvus::proto::plan::OpType::Match));
    EXPECT_FALSE(
        int_index.ShouldUseOp(milvus::proto::plan::OpType::PrefixMatch));
    EXPECT_FALSE(
        int_index.ShouldUseOp(milvus::proto::plan::OpType::PostfixMatch));
    EXPECT_FALSE(
        int_index.ShouldUseOp(milvus::proto::plan::OpType::InnerMatch));
    EXPECT_FALSE(
        int_index.ShouldUseOp(milvus::proto::plan::OpType::RegexMatch));
    EXPECT_TRUE(int_index.ShouldUseOp(milvus::proto::plan::OpType::Equal));

    milvus::index::BitmapIndex<std::string> string_index;
    EXPECT_TRUE(string_index.ShouldUseOp(milvus::proto::plan::OpType::Match));
    EXPECT_TRUE(
        string_index.ShouldUseOp(milvus::proto::plan::OpType::PrefixMatch));
    EXPECT_TRUE(
        string_index.ShouldUseOp(milvus::proto::plan::OpType::PostfixMatch));
    EXPECT_TRUE(
        string_index.ShouldUseOp(milvus::proto::plan::OpType::InnerMatch));
    EXPECT_TRUE(
        string_index.ShouldUseOp(milvus::proto::plan::OpType::RegexMatch));
    EXPECT_TRUE(string_index.ShouldUseOp(milvus::proto::plan::OpType::Equal));
}

TYPED_TEST_P(TypedScalarIndexTest, In) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_in<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, NotIn) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_not_in<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Reverse) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_reverse<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Range) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_range<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Codec) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());

        auto create_index_result = index->UploadUnified({});
        auto index_files = create_index_result->GetIndexFiles();
        auto copy_index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
        milvus::Config load_config;
        load_config["index_files"] = index_files;
        load_config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        copy_index->LoadUnified(load_config);

        auto copy_scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(copy_index.get());
        ASSERT_EQ(nb, copy_scalar_index->Count());
        assert_in<T>(copy_scalar_index, arr);
        assert_not_in<T>(copy_scalar_index, arr);
        assert_range<T>(copy_scalar_index, arr);
    }
}

// TODO: it's easy to overflow for int8_t. Design more reasonable ut.
using ScalarT =
    ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double>;

REGISTER_TYPED_TEST_SUITE_P(TypedScalarIndexTest,
                            Dummy,
                            Constructor,
                            Count,
                            In,
                            NotIn,
                            Range,
                            Codec,
                            Reverse,
                            HasRawData);

INSTANTIATE_TYPED_TEST_SUITE_P(ArithmeticCheck, TypedScalarIndexTest, ScalarT);

template <typename T>
class TypedScalarIndexTestV2 : public ::testing::Test {
 public:
    struct Helper {};

 protected:
};

static std::unordered_map<std::type_index,
                          const std::shared_ptr<arrow::DataType>>
    m_fields = {{typeid(int8_t), arrow::int8()},
                {typeid(int16_t), arrow::int16()},
                {typeid(int32_t), arrow::int32()},
                {typeid(int64_t), arrow::int64()},
                {typeid(float), arrow::float32()},
                {typeid(double), arrow::float64()}};

template <typename T>
std::shared_ptr<arrow::Schema>
TestSchema(int vec_size) {
    arrow::FieldVector fields;
    fields.push_back(arrow::field("pk", arrow::int64()));
    fields.push_back(arrow::field("ts", arrow::int64()));
    fields.push_back(arrow::field("scalar", m_fields[typeid(T)]));
    fields.push_back(arrow::field("vec", arrow::fixed_size_binary(vec_size)));
    return std::make_shared<arrow::Schema>(fields);
}

template <typename T>
std::shared_ptr<arrow::RecordBatchReader>
TestRecords(int vec_size, GeneratedData& dataset, std::vector<T>& scalars) {
    arrow::Int64Builder pk_builder;
    arrow::Int64Builder ts_builder;
    arrow::NumericBuilder<typename TypedScalarIndexTestV2<T>::Helper::C>
        scalar_builder;
    arrow::FixedSizeBinaryBuilder vec_builder(
        arrow::fixed_size_binary(vec_size));
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto data = reinterpret_cast<char*>(xb_data.data());
    for (auto i = 0; i < nb; ++i) {
        EXPECT_TRUE(pk_builder.Append(i).ok());
        EXPECT_TRUE(ts_builder.Append(i).ok());
        EXPECT_TRUE(vec_builder.Append(data + i * vec_size).ok());
    }
    for (auto& v : scalars) {
        EXPECT_TRUE(scalar_builder.Append(v).ok());
    }
    std::shared_ptr<arrow::Array> pk_array;
    EXPECT_TRUE(pk_builder.Finish(&pk_array).ok());
    std::shared_ptr<arrow::Array> ts_array;
    EXPECT_TRUE(ts_builder.Finish(&ts_array).ok());
    std::shared_ptr<arrow::Array> scalar_array;
    EXPECT_TRUE(scalar_builder.Finish(&scalar_array).ok());
    std::shared_ptr<arrow::Array> vec_array;
    EXPECT_TRUE(vec_builder.Finish(&vec_array).ok());
    auto schema = TestSchema<T>(vec_size);
    auto rec_batch = arrow::RecordBatch::Make(
        schema, nb, {pk_array, ts_array, scalar_array, vec_array});
    auto reader =
        arrow::RecordBatchReader::Make({rec_batch}, schema).ValueOrDie();
    return reader;
}

template <>
struct TypedScalarIndexTestV2<int8_t>::Helper {
    using C = arrow::Int8Type;
};

template <>
struct TypedScalarIndexTestV2<int16_t>::Helper {
    using C = arrow::Int16Type;
};

template <>
struct TypedScalarIndexTestV2<int32_t>::Helper {
    using C = arrow::Int32Type;
};

template <>
struct TypedScalarIndexTestV2<int64_t>::Helper {
    using C = arrow::Int64Type;
};

template <>
struct TypedScalarIndexTestV2<float>::Helper {
    using C = arrow::FloatType;
};

template <>
struct TypedScalarIndexTestV2<double>::Helper {
    using C = arrow::DoubleType;
};

using namespace milvus::index;
template <typename T>
std::vector<T>
GenerateRawData(int N, int cardinality) {
    using std::vector;
    std::default_random_engine random(60);
    std::normal_distribution<> distr(0, 1);
    vector<T> data(N);
    for (auto& x : data) {
        x = random() % (cardinality);
    }
    return data;
}

template <>
std::vector<std::string>
GenerateRawData(int N, int cardinality) {
    using std::vector;
    std::default_random_engine random(60);
    std::normal_distribution<> distr(0, 1);
    vector<std::string> data(N);
    for (auto& x : data) {
        x = std::to_string(random() % (cardinality));
    }
    return data;
}

template <typename T>
IndexBasePtr
TestBuildIndex(int N, int cardinality, int index_type) {
    auto raw_data = GenerateRawData<T>(N, cardinality);
    if (index_type == 0) {
        auto index = std::make_unique<milvus::index::BitmapIndex<T>>();
        index->Build(N, raw_data.data());
        return index;
    } else if (index_type == 1) {
        if constexpr (std::is_same_v<T, std::string>) {
            auto index = std::make_unique<milvus::index::StringIndexMarisa>();
            index->Build(N, raw_data.data());
            return index;
        } else {
            auto index = milvus::index::CreateScalarIndexSort<T>();
            index->Build(N, raw_data.data());
            return index;
        }
    }
    throw std::invalid_argument("unsupported index_type");
}

template <typename T>
void
TestIndexSearchIn() {
    // low data cardinality
    {
        int N = 1000;
        std::vector<int> data_cardinality = {10, 20, 100};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());
            std::vector<T> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(static_cast<T>(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }

    // high data cardinality
    {
        int N = 10000;
        std::vector<int> data_cardinality = {1001, 2000};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());
            std::vector<T> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(static_cast<T>(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
}

template <>
void
TestIndexSearchIn<std::string>() {
    // low data cardinality
    {
        int N = 1000;
        std::vector<int> data_cardinality = {10, 20, 100};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<std::string>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<std::string>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(sort_index.get());
            std::vector<std::string> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(std::to_string(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
    // high data cardinality
    {
        int N = 10000;
        std::vector<int> data_cardinality = {1001, 2000};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<std::string>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<std::string>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(sort_index.get());
            std::vector<std::string> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(std::to_string(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
}

TEST(ScalarTest, test_function_In) {
    TestIndexSearchIn<int8_t>();
    TestIndexSearchIn<int16_t>();
    TestIndexSearchIn<int32_t>();
    TestIndexSearchIn<int64_t>();
    TestIndexSearchIn<float>();
    TestIndexSearchIn<double>();
    TestIndexSearchIn<std::string>();
}

template <typename T>
void
TestIndexSearchRange() {
    // low data cordinality
    {
        int N = 1000;
        std::vector<int> data_cardinality = {10, 20, 100};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());

            auto final1 = bitmap_index_ptr->Range(10, milvus::OpType::LessThan);
            auto final2 = sort_index_ptr->Range(10, milvus::OpType::LessThan);
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->Range(10, true, 100, false);
            auto final4 = sort_index_ptr->Range(10, true, 100, false);
            EXPECT_EQ(final3.size(), final4.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }

    // high data cordinality
    {
        int N = 10000;
        std::vector<int> data_cardinality = {1001, 2000};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());

            auto final1 = bitmap_index_ptr->Range(10, milvus::OpType::LessThan);
            auto final2 = sort_index_ptr->Range(10, milvus::OpType::LessThan);
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->Range(10, true, 100, false);
            auto final4 = sort_index_ptr->Range(10, true, 100, false);
            EXPECT_EQ(final3.size(), final4.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
}

TEST(ScalarTest, test_function_range) {
    TestIndexSearchRange<int8_t>();
    TestIndexSearchRange<int16_t>();
    TestIndexSearchRange<int32_t>();
    TestIndexSearchRange<int64_t>();
    TestIndexSearchRange<float>();
    TestIndexSearchRange<double>();
}

// ScalarIndex<T>::IsNotNull(int64_t) -- the row-count-aware overload that
// projects absolute null offsets into a caller-owned row space -- is hidden in
// every subclass that declares IsNotNull() without a using-declaration, so a
// call through the derived static type simply stops compiling. Each subclass
// carries `using <base>::IsNotNull;` for exactly that reason. Nothing at
// runtime can catch its removal, so pin it here.
template <typename Index, typename = void>
struct HasRowCountIsNotNull : std::false_type {};

template <typename Index>
struct HasRowCountIsNotNull<
    Index,
    std::void_t<decltype(std::declval<Index&>().IsNotNull(int64_t{}))>>
    : std::true_type {};

static_assert(HasRowCountIsNotNull<milvus::index::ScalarIndex<int64_t>>::value,
              "the base overload must exist");
static_assert(HasRowCountIsNotNull<milvus::index::BitmapIndex<int64_t>>::value);
static_assert(
    HasRowCountIsNotNull<milvus::index::HybridScalarIndex<int64_t>>::value);
static_assert(
    HasRowCountIsNotNull<milvus::index::InvertedIndexTantivy<int64_t>>::value);
static_assert(HasRowCountIsNotNull<
              milvus::index::JsonFlatIndexQueryExecutor<std::string>>::value);
static_assert(
    HasRowCountIsNotNull<milvus::index::ScalarIndexSort<int64_t>>::value);
static_assert(HasRowCountIsNotNull<milvus::index::StringIndexMarisa>::value);
static_assert(HasRowCountIsNotNull<milvus::index::StringIndexSort>::value);
static_assert(HasRowCountIsNotNull<milvus::index::JsonKeyStats>::value);
static_assert(HasRowCountIsNotNull<milvus::index::FMIndex>::value);

namespace {
template <typename Index>
class PackedLoadAccess : public Index {
 public:
    using Index::FinishLoadAsync;
    using Index::Index;
    using Index::PlanLoad;
};

milvus::storage::IndexEntryDirectory
ErrorTestDirectory(
    std::initializer_list<std::pair<std::string, size_t>> entries) {
    nlohmann::json directory = {{"entries", nlohmann::json::array()}};
    size_t offset = 0;
    for (const auto& [name, bytes] : entries) {
        directory["entries"].push_back({{"name", name},
                                        {"offset", offset},
                                        {"size", bytes},
                                        {"crc32", "00000000"}});
        offset += bytes;
    }
    const auto json = directory.dump();
    return milvus::storage::ParseIndexEntryDirectory(
               std::span(reinterpret_cast<const uint8_t*>(json.data()),
                         json.size()),
               json.size() + offset + milvus::storage::MILVUS_V3_MAGIC_SIZE +
                   milvus::storage::MILVUS_V3_FOOTER_SIZE)
        .first;
}

template <typename F>
void
ExpectPackedLoadError(milvus::ErrorCode expected, F&& load) {
    try {
        load();
        FAIL() << "expected classified packed load error";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), expected);
        auto status = milvus::FailureCStatus(&error);
        EXPECT_EQ(status.error_code, static_cast<int>(expected));
        free(const_cast<char*>(status.error_msg));
    }
}
}  // namespace

TEST(ScalarIndexV3ErrorCodeTest, PersistedLengthsAreDataFormatErrors) {
    using namespace milvus;
    using namespace milvus::index;
    const auto ctx = GetTempFileManagerCtx(Int64);
    PackedLoadAccess<ScalarIndexSort<int64_t>> sort(ctx);
    Config config{{ENABLE_MMAP, false}};
    nlohmann::json sort_meta{
        {"index_length", 1}, {"num_rows", 1}, {"is_nested", false}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        sort.PlanLoad(
            ErrorTestDirectory({{"index_data", 1}}), sort_meta, config);
    });
    ExpectPackedLoadError(DataFormatBroken, [&] {
        sort.PlanLoad(
            ErrorTestDirectory({{"index_data", sizeof(IndexStructure<int64_t>)},
                                {"idx_to_offsets", 1},
                                {"valid_bitset", 1}}),
            sort_meta,
            config);
    });

    PackedLoadAccess<StringIndexSort> strings(GetTempFileManagerCtx(VarChar));
    const nlohmann::json string_meta{
        {"version", StringIndexSort::SERIALIZATION_VERSION},
        {"num_rows", 8},
        {"is_nested", false}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        strings.PlanLoad(
            ErrorTestDirectory({{"index_data", 1}, {"valid_bitset", 2}}),
            string_meta,
            {});
    });
    PackedLoadAccess<BitmapIndex<int64_t>> bitmap(ctx);
    const nlohmann::json bitmap_meta{{BITMAP_INDEX_LENGTH, 1},
                                     {BITMAP_INDEX_NUM_ROWS, 8}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        bitmap.PlanLoad(ErrorTestDirectory({{BITMAP_INDEX_DATA, 1},
                                            {BITMAP_INDEX_VALID_BITSET, 2}}),
                        bitmap_meta,
                        {});
    });
    PackedLoadAccess<StringIndexMarisa> marisa(GetTempFileManagerCtx(VarChar));
    ExpectPackedLoadError(DataFormatBroken, [&] {
        marisa.PlanLoad(
            ErrorTestDirectory({{MARISA_TRIE_INDEX, 1},
                                {MARISA_STR_IDS, sizeof(int64_t)},
                                {MARISA_CSR_INDEX, sizeof(uint32_t)}}),
            {},
            {});
    });
    PackedLoadAccess<NgramInvertedIndex> ngram(GetTempFileManagerCtx(VarChar),
                                               NgramParams{true, 2, 3});
    ExpectPackedLoadError(DataFormatBroken, [&] {
        ngram.PlanLoad(ErrorTestDirectory({{"engine_file", 1},
                                           {NGRAM_AVG_ROW_SIZE_FILE_NAME, 1}}),
                       {{"has_null", false}, {"file_names", {"engine_file"}}},
                       {});
    });
    IndexLoadPlan plan;
    ExpectPackedLoadError(DataFormatBroken, [&] {
        AppendJsonNonExistOffsetsPlan(
            plan,
            ErrorTestDirectory({{INDEX_NON_EXIST_OFFSET_FILE_NAME, 1}}),
            {{"has_non_exist", true}});
    });
}

TEST(ScalarIndexV3ErrorCodeTest, OptionalMetadataValidatesPresentTypes) {
    using namespace milvus;
    using namespace milvus::index;
    PackedLoadAccess<BitmapIndex<int64_t>> bitmap(GetTempFileManagerCtx(Int64));
    const auto directory = ErrorTestDirectory({{BITMAP_INDEX_DATA, 0}});
    nlohmann::json metadata{{BITMAP_INDEX_LENGTH, 0},
                            {BITMAP_INDEX_NUM_ROWS, 0}};
    EXPECT_NO_THROW(bitmap.PlanLoad(directory, metadata, {}));
    metadata["is_nested"] = "true";
    ExpectPackedLoadError(DataFormatBroken,
                          [&] { bitmap.PlanLoad(directory, metadata, {}); });
    IndexLoadPlan plan;
    EXPECT_NO_THROW(AppendJsonNonExistOffsetsPlan(plan, directory, {}));
    ExpectPackedLoadError(DataFormatBroken, [&] {
        AppendJsonNonExistOffsetsPlan(
            plan, directory, {{"has_non_exist", "false"}});
    });
    ExpectPackedLoadError(DataFormatBroken, [&] {
        AppendJsonNonExistOffsetsPlan(
            plan, directory, {{"has_non_exist", true}});
    });
}

TEST(ScalarIndexV3ErrorCodeTest, UnsupportedFormatVersionKeepsItsCode) {
    using namespace milvus;
    using namespace milvus::index;
    PackedLoadAccess<StringIndexSort> strings(GetTempFileManagerCtx(VarChar));
    ExpectPackedLoadError(Unsupported, [&] {
        strings.PlanLoad(ErrorTestDirectory({}), {{"version", 9999}}, {});
    });
    PackedLoadAccess<StringIndexMarisa> marisa(GetTempFileManagerCtx(VarChar));
    ExpectPackedLoadError(Unsupported, [&] {
        marisa.PlanLoad(
            ErrorTestDirectory({{MARISA_TRIE_INDEX, 1},
                                {MARISA_STR_IDS, sizeof(int64_t)},
                                {MARISA_CSR_INDEX, sizeof(uint32_t)},
                                {MARISA_CSR_OFFSETS, 0}}),
            {{"csr_num_keys", 0}, {"marisa_csr_format_version", 9999}},
            {});
    });
}

TEST(ScalarIndexV3ErrorCodeTest, UnrecognizedHybridMetadataIsDataFormatError) {
    using namespace milvus;
    using namespace milvus::index;
    const Config config{{INDEX_FILES, {"milvus_packed_hybrid_index.v3"}}};
    ExpectPackedLoadError(DataFormatBroken,
                          [&] { ResolvePackedHybridIndexType({}, config); });
    // Old standalone files still identify their type without hybrid metadata.
    EXPECT_EQ(ResolvePackedHybridIndexType(
                  {}, {{INDEX_FILES, {"milvus_packed_stlsort_index.v3"}}}),
              ScalarIndexType::STLSORT);
}

TEST(ScalarIndexV3ErrorCodeTest, MmapFailureKeepsItsCodeAndCleansTargets) {
    using namespace milvus;
    using namespace milvus::index;
    PackedLoadAccess<ScalarIndexSort<int64_t>> sort(
        GetTempFileManagerCtx(Int64));
    std::vector<std::string> paths;
    {
        auto plan = sort.PlanLoad(
            ErrorTestDirectory({{"index_data", 0},
                                {"idx_to_offsets", 0},
                                {"valid_bitset", 0}}),
            {{"index_length", 0}, {"num_rows", 0}, {"is_nested", false}},
            {{ENABLE_MMAP, true}});
        for (const auto& entry : plan.entries) {
            if (const auto* file =
                    std::get_if<storage::FileEntryTarget>(&entry.target)) {
                paths.push_back(file->staging->path);
                std::filesystem::create_directories(
                    std::filesystem::path(file->staging->path).parent_path());
                file->staging->Prepare(storage::io::Priority::HIGH);
                file->staging->Finish();
            }
        }
        // mmap(length=0) deterministically fails with EINVAL for the offsets file.
        ExpectPackedLoadError(MmapError, [&] {
            folly::coro::blockingWait(sort.FinishLoadAsync(plan, {}));
        });
    }
    ASSERT_EQ(paths.size(), 2);
    for (const auto& path : paths) {
        EXPECT_FALSE(std::filesystem::exists(path));
    }
}
