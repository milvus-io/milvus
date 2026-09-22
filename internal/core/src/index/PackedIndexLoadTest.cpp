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

#include "index/test_utils/LoaderTestAccess.h"
#include "index/IndexLoaderFactory.h"
#include "index/PackedIndexLoad.h"
#include <thread>

#include <gtest/gtest.h>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <array>
#include <numeric>
#include "folly/executors/ManualExecutor.h"
#include "folly/ScopeGuard.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileWriter.h"
#include "index/IndexTypeAdapter.h"
#include "common/OpContext.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "index/Meta.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/bitmap/BitmapIndexLoader.h"
#include "index/scalar/marisa/MarisaIndexLoader.h"
#include "index/scalar/ngram/NgramIndexLoader.h"
#include "index/scalar/sort/SortedIndexLoader.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/scalar/sort/SortedIndexReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index::test {
namespace {
storage::LoadOptions
ScalarOptions(DataType type) {
    storage::LoadOptions opts;
    opts.params = {{"field_type", type},
                   {"value_type", type},
                   {"nested", false},
                   {"nullable", true}};
    return opts;
}

void
PlanProjection(IndexLoadPlan& plan,
               const storage::IndexEntryDirectory& directory,
               const nlohmann::json& metadata) {
    storage::LoadOptions opts;
    opts.params = AnnotateJsonProjectionCompleteness(
        ScalarReaderBackends()
            .Get<double>("JsonProjectedSortedDouble")
            .LoadParams({}),
        directory,
        metadata);
    static_cast<void>(PreparePackedJsonProjectedOpen(
        families::kSort, directory, metadata, opts, plan));
}

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

TEST(ScalarIndexV3AsyncTest, HeapAndMmapPreserveValuesAndNulls) {
    for (const auto* name : {"BitmapVarchar",
                             "BitmapVarcharMmap",
                             "SortedVarchar",
                             "SortedVarcharMmap",
                             "InvertedVarchar",
                             "InvertedVarcharMmap",
                             "MarisaVarchar",
                             "MarisaVarcharMmap",
                             "FmIndexVarchar",
                             "FmIndexVarcharMmap"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data(
            {"alpha", "beta", "beta", "null"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 4});
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = 4}, true);
        const auto* pattern =
            dynamic_cast<const IPatternMatchReader*>(reader.get());
        ASSERT_NE(pattern, nullptr);
        ExpectHits(
            pattern->PatternMatch("beta", PatternOp::PrefixMatch), 4, {1, 2});
        ExpectHits(
            pattern->PatternMatch("null", PatternOp::PrefixMatch), 4, {});
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ExpectHits(nulls->IsNull(), 4, {3});
    }
}

TEST(ScalarIndexV3AsyncTest, NgramCandidatesAndNulls) {
    for (const auto* name :
         {"NgramVarcharMin2Max4Heap", "NgramVarcharMin2Max4Mmap"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data(
            {"alpha", "beta", "alphabet", "alpha"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 4});
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = 4}, true);
        const auto* ngram = dynamic_cast<const INgramReader*>(reader.get());
        ASSERT_NE(ngram, nullptr);
        ASSERT_TRUE(ngram->CanHandle("alph", PatternOp::PrefixMatch));
        TargetBitmap candidates(4, true);
        ngram->Candidates("alph", PatternOp::PrefixMatch, candidates);
        // These particular inputs have no gram collisions; candidates are not
        // generally exact matches for arbitrary input.
        ExpectHits(candidates, 4, {0, 2});
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ExpectHits(nulls->IsNull(), 4, {3});
    }
}

TEST(ScalarIndexV3AsyncTest, SpatialCandidatesAndNulls) {
    for (const auto* name : {"SpatialRTreeHeap", "SpatialRTreeMmapRequested"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        auto wkb = [](const char* text) {
            return Geometry(GetThreadLocalGEOSContext(), text).to_wkb_string();
        };
        ScalarTestData<std::string_view> data({wkb("POINT(0 0)"),
                                               wkb("POINT(1 1)"),
                                               wkb("POINT(2 2)"),
                                               wkb("POINT(0 0)")});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 4});
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = 4}, true);
        const auto* spatial = dynamic_cast<const ISpatialReader*>(reader.get());
        ASSERT_NE(spatial, nullptr);
        Geometry query(GetThreadLocalGEOSContext(), "POINT(0 0)");
        ExpectHits(spatial->Candidates(SpatialOp::Intersects, query), 4, {0});
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ExpectHits(nulls->IsNull(), 4, {3});
    }
}

TEST(ScalarIndexV3AsyncTest, TextMatchesAndNulls) {
    for (const auto* name : {"TextVarcharV7Heap", "TextVarcharV7Mmap"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data(
            {"alpha beta", "beta", "alpha", "alpha"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 4});
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = 4}, true);
        const auto* text = dynamic_cast<const ITextMatchReader*>(reader.get());
        ASSERT_NE(text, nullptr);
        ExpectHits(text->MatchQuery("alpha", 1), 4, {0, 2});
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ExpectHits(nulls->IsNull(), 4, {3});
    }
}

TEST(ScalarIndexV3AsyncTest, JsonPathsValuesAndNulls) {
    for (const auto* name : {"JsonFlatV7", "JsonFlatV7Mmap"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data({R"({"a":10})",
                                               R"({"a":20})",
                                               R"({"b":30})",
                                               R"({"a":10})",
                                               R"({"a":null})"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 5});
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = 5}, true);
        const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
        ASSERT_NE(json, nullptr);
        // Flat-index existence follows indexed values, excluding JSON null.
        ExpectHits(json->Exists("/a"), 5, {0, 1});
        auto resolved = json->Resolve("/a", JsonCastType::FromString("DOUBLE"));
        ASSERT_TRUE(resolved);
        const auto* predicate =
            dynamic_cast<const IScalarPredicateReader<double>*>(resolved.get());
        ASSERT_NE(predicate, nullptr);
        const double value = 10;
        ExpectHits(predicate->In(1, &value), 5, {0});
        const auto* path_nulls =
            dynamic_cast<const INullReader*>(resolved.get());
        ASSERT_NE(path_nulls, nullptr);
        ExpectHits(path_nulls->IsNull(), 5, {2, 3, 4});
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ExpectHits(nulls->IsNull(), 5, {3});
    }
}

namespace {
// Gate real serialized payload slices after Open has finished. Keep every
// destination alive until all pending reads are explicitly completed.
void
CheckControlledPackedLoad(bool cancel_first) {
    milvus::test::ScopedLoadTransientBudget budget(0);
    auto& admission = storage::LoadAdmissionController::GetInstance();
    const auto old_slots = admission.CapacitySlots();
    admission.SetCapacitySlots(8);
    auto restore =
        folly::makeGuard([&] { admission.SetCapacitySlots(old_slots); });
    const auto slice_size = storage::DefaultStreamSliceSize();
    std::string large(slice_size + 257, 'a');
    for (size_t i = 0; i < large.size(); ++i)
        large[i] = static_cast<char>('a' + i % 23);
    ScalarTestData<std::string_view> data(
        {"zulu", large, "alpha", "null", "omega"});
    data.validity.reset(3);
    const ScalarTestInput<std::string_view> input(data);
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    auto artifact = backend.Build(input.View(), {.row_count = 5});
    const auto persisted = SerializeV3(*artifact);
    const std::array<std::string_view, 5> expected{
        "zulu", large, "alpha", "null", "omega"};
    auto run = []<typename T>(folly::coro::Task<T> task) {
        return folly::coro::blockingWait(folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor(
                {}, proto::common::LoadPriority::HIGH),
            std::move(task)));
    };
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto staging = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "controlled-load-XXXXXX",
            "loader test");
        const auto packed = MakePackedArtifactBuffer(persisted);
        milvus::test::ControlledDirectReadFile* remote = nullptr;
        auto source = std::shared_ptr<storage::AsyncIndexEntryReader>(
            milvus::test::OpenDirectIndexEntryReader(
                std::vector<uint8_t>(packed->data(),
                                     packed->data() + packed->size()),
                &remote));
        size_t expected_reads = 0;
        for (const auto name : {sort_format::kIndexData,
                                sort_format::kValidBitset,
                                sort_format::kIdxToOffsets}) {
            if (source->Directory().HasEntry(name)) {
                const auto bytes = source->Directory().At(name).plaintext_size;
                expected_reads += (bytes + slice_size - 1) / slice_size;
            }
        }
        ASSERT_GE(
            source->Directory().At(sort_format::kIndexData).plaintext_size,
            slice_size + 1);
        ASSERT_LE(expected_reads, 8);
        storage::LoadOptions options;
        options.params = backend.LoadParams({.row_count = 5});
        options.enable_mmap = mmap;
        options.mmap_dir_path = staging->Path();
        auto loader = run(SortedIndexLoader::Open(
            {OpenedIndexInput{PackedIndexSource{source}}, options}));
        remote->SetAutoComplete(false);
        folly::CancellationSource cancel;
        OpContext context(cancel.getToken());
        auto loading = std::async(std::launch::async,
                                  [&] { return run(loader->Load(&context)); });
        auto drain = folly::makeGuard([&] {
            cancel.requestCancellation();
            remote->SetAutoComplete(true);
            const auto count = remote->DirectReadCalls().size();
            for (size_t i = 0; i < count; ++i) remote->Complete(i);
            if (loading.valid())
                loading.wait();
        });
        ASSERT_TRUE(remote->WaitForCallCount(expected_reads));
        const auto calls = remote->DirectReadCalls();
        ASSERT_EQ(calls.size(), expected_reads);
        std::vector<std::pair<int64_t, size_t>> order;
        for (size_t i = 0; i < calls.size(); ++i)
            order.emplace_back(calls[i].position, i);
        std::sort(order.rbegin(), order.rend());
        if (cancel_first)
            cancel.requestCancellation();
        for (size_t i = 0; i + 1 < order.size(); ++i)
            remote->Complete(order[i].second);
        EXPECT_EQ(loading.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        remote->Complete(order.back().second);
        IIndexReaderBasePtr reader;
        if (cancel_first) {
            ExpectPackedLoadError(FollyCancel,
                                  [&] { static_cast<void>(loading.get()); });
            EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
            remote->SetAutoComplete(true);
            OpContext retry_context;
            reader = run(loader->Load(&retry_context));
        } else {
            reader = loading.get();
        }
        drain.dismiss();
        loader.reset();
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->Count(), 5);
        const auto* values =
            dynamic_cast<const IScalarValueReader<std::string_view>*>(
                reader.get());
        ASSERT_NE(values, nullptr);
        for (size_t i = 0; i < expected.size(); ++i) {
            SCOPED_TRACE(i);
            const auto actual = values->Lookup(i);
            if (i == 3)
                EXPECT_FALSE(actual.has_value());
            else {
                ASSERT_TRUE(actual.has_value());
                EXPECT_TRUE(*actual == expected[i]);
            }
        }
        const auto* pattern =
            dynamic_cast<const IPatternMatchReader*>(reader.get());
        ASSERT_NE(pattern, nullptr);
        ExpectHits(
            pattern->PatternMatch("alpha", PatternOp::PrefixMatch), 5, {2});
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ExpectHits(nulls->IsNull(), 5, {3});
        reader.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
    }
}
}  // namespace

TEST(ScalarIndexV3AsyncTest, ReversedPayloadSlicesPreserveRowsAndValues) {
    CheckControlledPackedLoad(false);
}

TEST(ScalarIndexV3AsyncTest, CancelledPayloadDrainsAndOpenedLoaderRetries) {
    CheckControlledPackedLoad(true);
}

TEST(ScalarIndexV3LoadTest, BitmapOpenDefersTargetsAndSupportsFreshLoads) {
    const auto old_mode = storage::FileWriter::GetMode();
    auto restore_mode = folly::makeGuard(
        [old_mode] { storage::FileWriter::SetMode(old_mode); });
    storage::FileWriter::SetMode(storage::FileWriter::WriteMode::DIRECT);
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    std::vector<std::string> values;
    for (size_t i = 0; i < 1000; ++i)
        values.push_back("value_" + std::to_string(i));
    ScalarTestData<std::string_view> data(std::move(values));
    const ScalarTestInput<std::string_view> rows(data);
    auto artifact = backend.Build(rows.View(), {.row_count = 1000});
    for (bool use_async : {false, true}) {
        for (bool mmap : {false, true}) {
            SCOPED_TRACE(use_async);
            SCOPED_TRACE(mmap);
            auto run = [use_async]<typename T>(folly::coro::Task<T> task) {
                if (use_async)
                    return folly::coro::blockingWait(
                        folly::coro::co_withExecutor(
                            storage::ResolveAsyncLoadExecutor(
                                {}, proto::common::LoadPriority::HIGH),
                            std::move(task)));
                return folly::coro::blockingWait(std::move(task));
            };
            auto staging = storage::LocalDirectory::CreateOwned(
                std::filesystem::temp_directory_path().string(),
                "open-load-XXXXXX",
                "loader test");
            auto bytes = MakePackedArtifactBuffer(SerializeV3(*artifact));
            auto input = std::make_shared<storage::RemoteInputStream>(
                std::make_shared<arrow::io::BufferReader>(bytes));
            OpenedIndexInput source;
            if (use_async) {
                source = PackedIndexSource{
                    std::shared_ptr<storage::AsyncIndexEntryReader>(
                        run(storage::AsyncIndexEntryReader::Open(
                            input, 0, proto::common::LoadPriority::HIGH)))};
            } else {
                source = PackedIndexSource{
                    std::shared_ptr<storage::IndexEntryReader>(
                        storage::IndexEntryReader::Open(input, input->Size()))};
            }
            storage::LoadOptions options;
            options.params = backend.LoadParams({.row_count = 1000});
            options.enable_mmap = mmap;
            options.mmap_dir_path = staging->Path();
            folly::CancellationSource opening;
            OpContext opening_context(opening.getToken());
            options.op_ctx = &opening_context;
            auto loader =
                run(BitmapIndexLoader::Open({std::move(source), options}));
            EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
            opening.requestCancellation();
            folly::CancellationSource first;
            OpContext first_context(first.getToken());
            // Low priority uses direct I/O; the second load defaults to buffered.
            first_context.runtime_load_priority = 1;
            auto reader1 = run(loader->Load(&first_context));
            first.requestCancellation();
            auto reader2 = run(loader->Load());
            loader.reset();
            for (const auto* reader : {reader1.get(), reader2.get()}) {
                EXPECT_EQ(reader->Count(), 1000);
                const auto* pattern =
                    dynamic_cast<const IPatternMatchReader*>(reader);
                ASSERT_NE(pattern, nullptr);
                ExpectHits(
                    pattern->PatternMatch("value_999", PatternOp::PrefixMatch),
                    1000,
                    {999});
            }
            reader1.reset();
            reader2.reset();
            EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
        }
    }
}

TEST(ScalarIndexV3LoadTest, FileTargetsDoNotMoveFamilyFinalizationToFilePool) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    ScalarTestData<int64_t> data({10, 20, 10});
    const ScalarTestInput<int64_t> rows(data);
    auto artifact = backend.Build(rows.View(), {.row_count = 3});
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(SerializeV3(*artifact))));
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "packed-executor-XXXXXX",
        "packed executor test");
    auto options = ScalarOptions(DataType::INT64);
    options.enable_mmap = true;
    options.mmap_dir_path = staging->Path();
    options.params["test_loading_thread"] =
        std::hash<std::thread::id>{}(std::this_thread::get_id());
    auto load = [&]() -> folly::coro::Task<IIndexReaderBasePtr> {
        auto opened = co_await storage::AsyncIndexEntryReader::Open(
            input, 0, proto::common::LoadPriority::HIGH);
        PackedIndexSource source{
            std::shared_ptr<storage::AsyncIndexEntryReader>(std::move(opened))};
        PackedAsyncFinishFn finish =
            [](IndexLoadPlan& plan,
               const storage::LoadOptions& opts,
               bool use_async) -> folly::coro::Task<IIndexReaderBasePtr> {
            EXPECT_EQ(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                      opts.params.at("test_loading_thread").get<size_t>());
            auto reader = co_await LoaderTestAccess::Finish<BitmapIndexLoader>(
                plan, opts, use_async);
            EXPECT_EQ(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                      opts.params.at("test_loading_thread").get<size_t>());
            co_return reader;
        };
        co_return co_await RunPackedIndexLoad(
            source,
            options,
            &LoaderTestAccess::Plan<BitmapIndexLoader>,
            finish,
            nullptr);
    };
    folly::ManualExecutor executor;
    auto future = folly::coro::co_withExecutor(&executor, load()).start();
    while (!future.isReady()) executor.drive();
    auto reader = std::move(future).get();
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 3);
    reader.reset();
    EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
}

TEST(ScalarIndexV3LoadTest, BitmapFrozenConversionYieldsBetweenBatches) {
    // Singleton postings consume 32 aligned frozen bytes each: just over 16 MiB.
    constexpr size_t count = (16 * 1024 * 1024 / 32) + 1;
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    std::vector<int64_t> values(count);
    std::iota(values.begin(), values.end(), int64_t{0});
    ScalarTestData<int64_t> data(std::move(values));
    const ScalarTestInput<int64_t> rows(data);
    auto artifact = backend.Build(rows.View(), {.row_count = count});
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(SerializeV3(*artifact))));
    auto source = storage::IndexEntryReader::Open(input, input->Size());
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "bitmap-batches-XXXXXX",
        "bitmap batch scheduling test");
    auto options = ScalarOptions(DataType::INT64);
    options.enable_mmap = true;
    options.mmap_dir_path = staging->Path();
    for (bool use_async : {false, true}) {
        SCOPED_TRACE(use_async);
        {
            auto plan = LoaderTestAccess::Plan<BitmapIndexLoader>(
                source->Directory(), source->IndexMeta(), options);
            for (const auto& entry : plan.entries) {
                const auto bytes = source->ReadEntry(entry.name).data;
                if (const auto* memory =
                        std::get_if<storage::MemoryEntryTarget>(
                            &entry.target)) {
                    std::memcpy(memory->data, bytes.data(), bytes.size());
                } else {
                    const auto& file =
                        std::get<storage::FileEntryTarget>(entry.target);
                    file.staging->Prepare(storage::io::Priority::HIGH);
                    file.staging->WriteAt(
                        file.offset, bytes.data(), bytes.size());
                    file.staging->Finish();
                }
            }
            auto task = LoaderTestAccess::Finish<BitmapIndexLoader>(
                plan, options, use_async);
            IIndexReaderBasePtr reader;
            if (use_async) {
                folly::ManualExecutor executor;
                auto future =
                    folly::coro::co_withExecutor(&executor, std::move(task))
                        .start();
                bool observed_batch = false;
                // File phases suspend onto another pool. Drive each resumed
                // conversion batch and observe progress before completion.
                while (!future.isReady()) {
                    executor.drive();
                    if (!future.isReady()) {
                        for (const auto& file :
                             std::filesystem::directory_iterator(
                                 staging->Path())) {
                            if (file.path().filename().string().find(
                                    "bitmap_frozen") != std::string::npos &&
                                file.file_size() >= 16 * 1024 * 1024) {
                                observed_batch = true;
                            }
                        }
                    }
                }
                EXPECT_TRUE(observed_batch);
                reader = std::move(future).get();
            } else {
                // No executor: sync conversion must not acquire a scheduling dependency.
                reader = folly::coro::blockingWait(std::move(task));
            }
            ASSERT_NE(reader, nullptr);
            EXPECT_EQ(reader->Count(), count);
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<int64_t>*>(
                    reader.get());
            ASSERT_NE(predicate, nullptr);
            for (int64_t key : {int64_t{0}, int64_t{count - 1}}) {
                ExpectHits(
                    predicate->In(1, &key), count, {static_cast<size_t>(key)});
            }
        }
        EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
    }
}

TEST(ScalarIndexV3LoadTest, FailedOpenDetachesBorrowedSourceContext) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    ScalarTestData<std::string_view> data({"alpha", "beta"});
    const ScalarTestInput<std::string_view> rows(data);
    auto artifact = backend.Build(rows.View(), {.row_count = 2});
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(SerializeV3(*artifact))));
    auto source = std::shared_ptr<storage::IndexEntryReader>(
        storage::IndexEntryReader::Open(input, input->Size()));
    storage::LoadOptions options;
    options.params = backend.LoadParams({.row_count = 2});
    options.params["field_type"] = DataType::NONE;
    options.params["value_type"] = DataType::NONE;
    folly::CancellationSource opening;
    OpContext context(opening.getToken());
    options.op_ctx = &context;
    IndexOpenRequest request{OpenedIndexInput{PackedIndexSource{source}},
                             options};
    ExpectPackedLoadError(DataTypeInvalid, [&] {
        static_cast<void>(folly::coro::blockingWait(
            BitmapIndexLoader::Open(std::move(request))));
    });
    opening.requestCancellation();
    // Direct reuse detects a token retained after metadata construction failed.
    EXPECT_GT(source->ReadEntry(BITMAP_INDEX_DATA).data.size(), 0);
}

TEST(ScalarIndexV3LoadTest, LoadIndexAsyncPreservesParentCancellation) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    ScalarTestData<std::string_view> data({"alpha", "beta"});
    const ScalarTestInput<std::string_view> values(data);
    auto artifact = backend.Build(values.View(), {.row_count = 2});
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(SerializeV3(*artifact))));
    const auto priority = proto::common::LoadPriority::HIGH;
    auto source = folly::coro::blockingWait(
        storage::AsyncIndexEntryReader::Open(input, 0, priority));
    storage::LoadOptions options;
    options.params = backend.LoadParams({.row_count = 2});
    const auto entry = LoaderRegistry::Instance().Lookup(backend.Family());
    folly::CancellationSource cancel;
    cancel.requestCancellation();
    ExpectPackedLoadError(FollyCancel, [&] {
        folly::coro::blockingWait(folly::coro::co_withCancellation(
            cancel.getToken(),
            LoadIndexAsync(entry,
                           {OpenedIndexInput{PackedIndexSource{
                                std::shared_ptr<storage::AsyncIndexEntryReader>(
                                    std::move(source))}},
                            options})));
    });
}

TEST(ScalarIndexV3ErrorCodeTest, CorruptPayloadCleansSyncAndAsyncTargets) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    ScalarTestData<std::string_view> data({"alpha", "beta"});
    const ScalarTestInput<std::string_view> values(data);
    auto artifact = backend.Build(values.View(), {.row_count = 2});
    for (const bool use_async : {false, true}) {
        SCOPED_TRACE(use_async);
        auto staging = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "packed-crc-XXXXXX",
            "packed CRC test");
        auto bytes = MakePackedArtifactBuffer(SerializeV3(*artifact));
        auto input = std::make_shared<storage::RemoteInputStream>(
            std::make_shared<arrow::io::BufferReader>(bytes));
        auto source = storage::IndexEntryReader::Open(input, input->Size());
        const auto& payload = std::get<storage::PlainEntrySource>(
            source->Directory().At(MARISA_TRIE_INDEX).source);
        bytes->mutable_data()[payload.remote_offset] ^= 1;
        storage::LoadOptions options;
        options.enable_mmap = true;
        options.mmap_dir_path = staging->Path();
        options.params = backend.LoadParams({.row_count = 2});
        const auto loader = LoaderRegistry::Instance().Lookup(backend.Family());
        ExpectPackedLoadError(DataFormatBroken, [&] {
            if (use_async) {
                auto load = [&]() -> folly::coro::Task<IIndexReaderBasePtr> {
                    const auto priority = proto::common::LoadPriority::HIGH;
                    auto async_source =
                        co_await storage::AsyncIndexEntryReader::Open(
                            input, 0, priority);
                    IndexOpenRequest request{
                        OpenedIndexInput{PackedIndexSource{
                            std::shared_ptr<storage::AsyncIndexEntryReader>(
                                std::move(async_source))}},
                        options};
                    co_return co_await LoadIndexAsync(loader,
                                                      std::move(request));
                };
                static_cast<void>(
                    folly::coro::blockingWait(folly::coro::co_withExecutor(
                        storage::ResolveAsyncLoadExecutor(
                            {}, proto::common::LoadPriority::HIGH),
                        load())));
            } else {
                static_cast<void>(
                    LoadIndex(loader,
                              {OpenedIndexInput{PackedIndexSource{
                                   std::shared_ptr<storage::IndexEntryReader>(
                                       std::move(source))}},
                               options}));
            }
        });
        EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
    }
}

TEST(ScalarIndexV3ErrorCodeTest, PersistedLengthsAreDataFormatErrors) {
    using namespace milvus;
    using namespace milvus::index;
    const auto numeric_opts = ScalarOptions(DataType::INT64);

    nlohmann::json sort_meta{
        {"index_length", 1}, {"num_rows", 1}, {"is_nested", false}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<SortedIndexLoader>(
            ErrorTestDirectory({{"index_data", 1}}), sort_meta, numeric_opts);
    });
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<SortedIndexLoader>(
            ErrorTestDirectory({{"index_data", sizeof(IndexStructure<int64_t>)},
                                {"idx_to_offsets", 1},
                                {"valid_bitset", 1}}),
            sort_meta,
            numeric_opts);
    });

    const auto string_opts = ScalarOptions(DataType::VARCHAR);
    const nlohmann::json string_meta{{"version", sort_format::kStringVersion},
                                     {"num_rows", 8},
                                     {"is_nested", false}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<SortedIndexLoader>(
            ErrorTestDirectory({{"index_data", 1}, {"valid_bitset", 2}}),
            string_meta,
            string_opts);
    });
    const auto bitmap_opts = ScalarOptions(DataType::INT64);
    const nlohmann::json bitmap_meta{{BITMAP_INDEX_LENGTH, 1},
                                     {BITMAP_INDEX_NUM_ROWS, 8}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<BitmapIndexLoader>(
            ErrorTestDirectory(
                {{BITMAP_INDEX_DATA, 1}, {BITMAP_INDEX_VALID_BITSET, 2}}),
            bitmap_meta,
            bitmap_opts);
    });
    const auto marisa_opts = ScalarOptions(DataType::VARCHAR);
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<MarisaIndexLoader>(
            ErrorTestDirectory({{MARISA_TRIE_INDEX, 1},
                                {MARISA_STR_IDS, sizeof(int64_t)},
                                {MARISA_CSR_INDEX, sizeof(uint32_t)}}),
            {},
            marisa_opts);
    });
    auto ngram_opts = ScalarOptions(DataType::VARCHAR);
    ngram_opts.params[MIN_GRAM] = 2;
    ngram_opts.params[MAX_GRAM] = 3;
    ngram_opts.params[SCALAR_INDEX_ENGINE_VERSION] = 3;
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<NgramIndexLoader>(
            ErrorTestDirectory({{"engine_file", 1}, {"ngram_avg_row_size", 1}}),
            {{"has_null", false}, {"file_names", {"engine_file"}}},
            ngram_opts);
    });
    IndexLoadPlan plan;
    ExpectPackedLoadError(DataFormatBroken, [&] {
        PlanProjection(
            plan,
            ErrorTestDirectory({{INDEX_NON_EXIST_OFFSET_FILE_NAME, 1}}),
            {{"has_non_exist", true}});
    });
}

TEST(ScalarIndexV3ErrorCodeTest, OptionalMetadataValidatesPresentTypes) {
    using namespace milvus;
    using namespace milvus::index;
    const auto bitmap_opts = ScalarOptions(DataType::INT64);
    const auto directory = ErrorTestDirectory({{BITMAP_INDEX_DATA, 0}});
    nlohmann::json metadata{{BITMAP_INDEX_LENGTH, 0},
                            {BITMAP_INDEX_NUM_ROWS, 0}};
    EXPECT_NO_THROW(LoaderTestAccess::Plan<BitmapIndexLoader>(
        directory, metadata, bitmap_opts));
    metadata["is_nested"] = "true";
    ExpectPackedLoadError(DataFormatBroken, [&] {
        LoaderTestAccess::Plan<BitmapIndexLoader>(
            directory, metadata, bitmap_opts);
    });
    IndexLoadPlan plan;
    EXPECT_NO_THROW(PlanProjection(plan, directory, {}));
    ExpectPackedLoadError(DataFormatBroken, [&] {
        PlanProjection(plan, directory, {{"has_non_exist", "false"}});
    });
    ExpectPackedLoadError(DataFormatBroken, [&] {
        PlanProjection(plan, directory, {{"has_non_exist", true}});
    });
}

TEST(ScalarIndexV3ErrorCodeTest, UnsupportedFormatVersionKeepsItsCode) {
    using namespace milvus;
    using namespace milvus::index;
    const auto string_opts = ScalarOptions(DataType::VARCHAR);
    ExpectPackedLoadError(Unsupported, [&] {
        LoaderTestAccess::Plan<SortedIndexLoader>(
            ErrorTestDirectory({}),
            {{"num_rows", 0}, {"version", 9999}},
            string_opts);
    });
    const auto marisa_opts = ScalarOptions(DataType::VARCHAR);
    ExpectPackedLoadError(Unsupported, [&] {
        LoaderTestAccess::Plan<MarisaIndexLoader>(
            ErrorTestDirectory({{MARISA_TRIE_INDEX, 1},
                                {MARISA_STR_IDS, sizeof(int64_t)},
                                {MARISA_CSR_INDEX, sizeof(uint32_t)},
                                {MARISA_CSR_OFFSETS, 0}}),
            {{"csr_num_keys", 0}, {"marisa_csr_format_version", 9999}},
            marisa_opts);
    });
}

TEST(ScalarIndexV3ErrorCodeTest, UnrecognizedHybridMetadataIsDataFormatError) {
    using namespace milvus;
    using namespace milvus::index;
    const Config config{{INDEX_FILES, {"milvus_packed_hybrid_index.v3"}}};
    ExpectPackedLoadError(DataFormatBroken, [&] {
        ResolvePackedLoadFamily(families::kHybrid, {}, config);
    });
    // Old standalone files still identify their type without hybrid metadata.
    EXPECT_EQ(ResolvePackedLoadFamily(
                  families::kHybrid,
                  {},
                  {{INDEX_FILES, {"milvus_packed_stlsort_index.v3"}}}),
              families::kSort);
}

TEST(ScalarIndexV3ErrorCodeTest, MmapFailureKeepsItsCodeAndCleansTargets) {
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "packed-error-XXXXXX",
        "packed mmap failure test");
    auto opts = ScalarOptions(DataType::INT64);
    opts.enable_mmap = true;
    opts.mmap_dir_path = staging->Path();
    std::vector<std::string> paths;
    {
        auto plan = LoaderTestAccess::Plan<SortedIndexLoader>(
            ErrorTestDirectory(
                {{"index_data", sizeof(IndexStructure<int64_t>)},
                 {"idx_to_offsets", sizeof(int32_t)},
                 {"valid_bitset", TargetBitmap(1, false).size_in_bytes()}}),
            {{"index_length", 1}, {"num_rows", 1}, {"is_nested", false}},
            opts);
        for (const auto& entry : plan.entries) {
            if (const auto* file =
                    std::get_if<storage::FileEntryTarget>(&entry.target)) {
                paths.push_back(file->staging->path);
                file->staging->Prepare(storage::io::Priority::HIGH);
                file->staging->Finish();
            }
        }
        ASSERT_EQ(paths.size(), 2);
        // /dev/null opens successfully but cannot be mapped. This reaches the
        // real mmap failure without relying on obsolete zero-row behavior.
        const auto& data =
            std::get<storage::FileEntryTarget>(plan.At("index_data").target);
        ASSERT_TRUE(std::filesystem::remove(data.staging->path));
        std::filesystem::create_symlink("/dev/null", data.staging->path);
        ExpectPackedLoadError(MmapError, [&] {
            LoaderTestAccess::Finish<SortedIndexLoader>(plan, opts);
        });
    }
    for (const auto& path : paths) {
        EXPECT_FALSE(std::filesystem::exists(path));
    }
    EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
}
}  // namespace milvus::index::test
