// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unistd.h>
#include <vector>

#include "common/ChunkWriter.h"
#include "common/GroupChunk.h"
#include "common/Utils.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "mmap/VortexColumn.h"
#include "mmap/VortexColumnGroup.h"
#include "milvus-storage/format/vortex/vortex_writer.h"
#include "milvus-storage/properties.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::segcore {
namespace {

template <typename Base>
class JsonTakeColumn : public Base {
 public:
    using Base::Base;

    mutable int take_calls = 0;

    ChunkedColumnInterface::TakeResultPtr
    Take(OpContext* ctx,
         ChunkedColumnInterface::TakeOptions options) const override {
        ++take_calls;
        EXPECT_EQ(options.pin_policy, TakePinPolicy::Batch);
        return Base::Take(ctx, std::move(options));
    }

    void
    BulkIsValid(OpContext*,
                std::function<void(bool, size_t)>,
                const int64_t*,
                int64_t) const override {
        ADD_FAILURE() << "JSON data and validity must come from the same Take";
    }
};

// 0: standalone Raw, 1: grouped Raw, 2: Vortex.
class SealedJsonTakeTest
    : public testing::TestWithParam<std::tuple<int, bool>> {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->set_primary_field_id(
            schema_->AddDebugField("pk", DataType::INT64));
        field_ = schema_->AddDebugField("json", DataType::JSON, nullable());
        const auto& meta = (*schema_)[field_];
        values_ = {R"({"v":0})",
                   R"({"v":null})",
                   R"({"other":2})",
                   R"({"v":3})",
                   "{\"v\":4,\"blob\":\"" + std::string(4096, 'x') + "\"}",
                   R"({"v":"中文"})",
                   R"({"v":[]})",
                   R"({"v":false})"};
        dir_ = std::filesystem::temp_directory_path() /
               ("milvus_json_take_" + std::to_string(getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));

        std::vector<std::unique_ptr<Chunk>> chunks;
        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        std::vector<VortexColumn::FileInfo> files;
        auto properties = std::make_shared<milvus_storage::api::Properties>();
        (*properties)[PROPERTY_FS_STORAGE_TYPE] = std::string("local");
        (*properties)[PROPERTY_FS_ROOT_PATH] = std::string("/");
        auto arrow_schema = arrow::schema({arrow::field(
            std::to_string(field_.get()), arrow::binary(), nullable())});
        for (int64_t start : {0, 4}) {
            arrow::BinaryBuilder builder;
            for (int64_t row = start; row < start + 4; ++row) {
                ASSERT_TRUE((valid(row) ? builder.Append(values_[row])
                                        : builder.AppendNull())
                                .ok());
            }
            std::shared_ptr<arrow::Array> array;
            ASSERT_TRUE(builder.Finish(&array).ok());
            if (backend() == 2) {
                std::filesystem::create_directories(dir_);
                const auto path =
                    (dir_ / (std::to_string(start) + ".vx")).string();
                auto writer_result =
                    milvus_storage::vortex::VortexFileWriter::Open(
                        std::make_shared<arrow::fs::LocalFileSystem>(),
                        arrow_schema,
                        path,
                        *properties);
                ASSERT_TRUE(writer_result.ok())
                    << writer_result.status().ToString();
                auto writer = std::move(writer_result).ValueOrDie();
                ASSERT_TRUE(writer
                                ->Write(arrow::RecordBatch::Make(
                                    arrow_schema, 4, {array}))
                                .ok());
                ASSERT_TRUE(writer->Flush().ok());
                auto closed = writer->Close();
                ASSERT_TRUE(closed.ok()) << closed.status().ToString();
                const auto file = std::move(closed).ValueOrDie();
                VortexColumn::FileInfo info;
                info.path = path;
                info.start_index = start;
                info.end_index = start + 4;
                info.file_size = file.Get<uint64_t>(
                    milvus_storage::api::kPropertyFileSize, 0);
                info.footer_size = file.Get<uint64_t>(
                    milvus_storage::api::kPropertyFooterSize, 0);
                files.push_back(std::move(info));
            } else {
                auto chunk = create_chunk(meta, {array}, false);
                if (backend() == 1) {
                    std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
                    fields.emplace(field_, std::move(chunk));
                    group_chunks.push_back(
                        std::make_unique<GroupChunk>(std::move(fields)));
                } else {
                    chunks.push_back(std::move(chunk));
                }
            }
        }
        if (backend() == 0) {
            auto slot =
                cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
                    std::make_unique<TestChunkTranslator>(
                        std::vector<int64_t>{4, 4},
                        "json_take",
                        std::move(chunks)),
                    nullptr);
            column_ =
                std::make_shared<JsonTakeColumn<ChunkedVariableColumn<Json>>>(
                    std::move(slot), meta);
        } else if (backend() == 1) {
            group_ = std::make_shared<ChunkedColumnGroup>(
                std::make_unique<TestGroupChunkTranslator>(
                    1,
                    std::vector<int64_t>{4, 4},
                    "json_take_group",
                    std::move(group_chunks)));
            column_ = std::make_shared<JsonTakeColumn<ProxyChunkColumn>>(
                group_, field_, meta);
        } else {
            auto group = std::make_shared<VortexColumnGroup>(
                files,
                properties,
                std::vector<std::string>{std::to_string(field_.get())},
                1,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                nullptr);
            column_ =
                std::make_shared<VortexColumn>(field_,
                                               meta,
                                               std::to_string(field_.get()),
                                               properties,
                                               std::move(group));
        }
        segment_ = std::make_unique<ChunkedSegmentSealedImpl>(
            schema_, nullptr, SegcoreConfig::default_config(), 710201);
        auto current = segment_->TestGetPublishedStateSnapshot();
        auto runtime = segment_->TestCloneMutableRuntimeResourceState();
        ChunkedSegmentSealedImpl::StateDelta initial;
        initial.runtime = segment_->TestFreezeRuntimeResourceState(runtime);
        auto staged = segment_->TestBuildNextPublishedState(current, initial);
        ChunkedSegmentSealedImpl::StateDelta delta;
        segment_->TestStageLoadFieldDataThenPublish(field_,
                                                    column_,
                                                    values_.size(),
                                                    DataType::JSON,
                                                    schema_,
                                                    runtime,
                                                    staged.get(),
                                                    current,
                                                    delta,
                                                    [] {});
    }

    void
    TearDown() override {
        segment_.reset();
        column_.reset();
        group_.reset();
        std::filesystem::remove_all(dir_);
    }

    int
    backend() const {
        return std::get<0>(GetParam());
    }
    bool
    nullable() const {
        return std::get<1>(GetParam());
    }
    bool
    valid(int64_t row) const {
        return !nullable() || row != 3;
    }

    int
    TakeCalls() const {
        if (backend() == 0) {
            return static_cast<JsonTakeColumn<ChunkedVariableColumn<Json>>*>(
                       column_.get())
                ->take_calls;
        }
        if (backend() == 1) {
            return static_cast<JsonTakeColumn<ProxyChunkColumn>*>(column_.get())
                ->take_calls;
        }
        return 0;
    }

    void
    Evict() {
        if (group_) {
            group_->ManualEvictCache();
        } else {
            column_->ManualEvictCache();
        }
    }

    SchemaPtr schema_;
    FieldId field_{0};
    std::vector<std::string> values_;
    std::filesystem::path dir_;
    std::shared_ptr<ChunkedColumnGroup> group_;
    std::shared_ptr<ChunkedColumnInterface> column_;
    std::unique_ptr<ChunkedSegmentSealedImpl> segment_;
};

TEST_P(SealedJsonTakeTest, RetrievalPreservesOrderValidityAndOwnership) {
    const auto before = TakeCalls();
    const std::vector<int64_t> offsets{7, 3, 0, 4, 2, 7, 1, 5, 6};
    auto output = segment_->bulk_subscript(
        nullptr, field_, offsets.data(), offsets.size());
    auto projected = segment_->bulk_subscript(nullptr,
                                              field_,
                                              offsets.data(),
                                              offsets.size(),
                                              std::vector<std::string>{"v"});
    std::vector<Json> escaped(offsets.size());
    TargetBitmap validity(offsets.size(), true);
    static_cast<SegmentInternalInterface*>(segment_.get())
        ->bulk_subscript(nullptr,
                         field_,
                         DataType::JSON,
                         offsets.data(),
                         offsets.size(),
                         escaped.data(),
                         validity);
    if (backend() != 2) {
        EXPECT_EQ(TakeCalls(), before + 3);
    }
    Evict();
    const std::vector<std::string> expected_projection{R"({"v":0})",
                                                       R"({"v":null})",
                                                       "{}",
                                                       R"({"v":3})",
                                                       R"({"v":4})",
                                                       R"({"v":"中文"})",
                                                       R"({"v":[]})",
                                                       R"({"v":false})"};
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto row = offsets[i];
        EXPECT_EQ(validity[i], valid(row));
        if (nullable()) {
            EXPECT_EQ(GetFieldDataRowValidData(*output).Get(i), valid(row));
            EXPECT_EQ(GetFieldDataRowValidData(*projected).Get(i), valid(row));
        }
        if (valid(row)) {
            EXPECT_EQ(output->scalars().json_data().data(i), values_[row]);
            EXPECT_EQ(projected->scalars().json_data().data(i),
                      expected_projection[row]);
            EXPECT_EQ(std::string_view(escaped[i]), values_[row]);
            EXPECT_NO_THROW((void)escaped[i].doc());
        }
    }
}

TEST_P(SealedJsonTakeTest, GetBorrowsMaterializedJsonWithoutAnotherCopy) {
    const std::vector<int64_t> offsets{7, 3, 0, 4, 7};
    auto take = column_->Take(nullptr,
                              {OffsetView::From(offsets.data(), offsets.size()),
                               TargetType::Json,
                               nullptr,
                               TakePinPolicy::Batch});
    ASSERT_NE(take, nullptr);
    const auto items = take->Access<Json>();
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto item = items[i];
        EXPECT_EQ(item.is_valid, valid(offsets[i]));
        EXPECT_EQ(item.value.has_value(), item.is_valid);
        EXPECT_FALSE(item.data_skipped);
        if (item.value) {
            EXPECT_EQ(std::string_view(*item.value), values_[offsets[i]]);
            EXPECT_NO_THROW((void)item.value->doc());
        }
    }
    const auto owned = take->GetOwn();
    ASSERT_NE(owned.owner, nullptr);
    const auto* values = owned.values.data_as<Json>();
    EXPECT_EQ(take->GetOwn().owner, owned.owner);
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto item = items[i];
        EXPECT_EQ(item.is_valid, valid(offsets[i]));
        EXPECT_EQ(item.value.has_value(), item.is_valid);
        EXPECT_FALSE(item.data_skipped);
        if (item.is_valid) {
            ASSERT_TRUE(item.value.has_value());
            EXPECT_EQ(item.value->data().data(), values[i].data().data());
            EXPECT_EQ(std::string_view(*item.value), values_[offsets[i]]);
        }
    }
    take.reset();
    Evict();
    for (size_t i = 0; i < offsets.size(); ++i) {
        if (valid(offsets[i])) {
            EXPECT_EQ(std::string_view(values[i]), values_[offsets[i]]);
            EXPECT_NO_THROW((void)values[i].doc());
        }
    }
}

TEST_P(SealedJsonTakeTest, FullCandidateMaskKeepsEveryInputPosition) {
    const int64_t offsets[]{7, 3, 4, 0, 7};
    TargetBitmap candidates(5, true);
    auto take = column_->Take(nullptr,
                              {OffsetView::From(offsets, 5),
                               TargetType::Json,
                               nullptr,
                               TakePinPolicy::Batch,
                               &candidates});
    ASSERT_NE(take, nullptr);
    EXPECT_EQ(take->Size(), 5);
    for (int64_t i = 0; i < 5; ++i) {
        const auto item = take->Get<Json>(i);
        EXPECT_EQ(item.is_valid, valid(offsets[i]));
        if (item.is_valid) {
            ASSERT_TRUE(item.value);
            EXPECT_EQ(std::string_view(*item.value), values_[offsets[i]]);
        }
    }
    const auto owned = take->GetOwn();
    take.reset();
    Evict();
    EXPECT_EQ(owned.size, 5);
    EXPECT_EQ(std::string_view(owned.values.data_as<Json>()[3]), values_[0]);
}

TEST_P(SealedJsonTakeTest, CandidateMaskExcludesFilesBeforePlanningAndDecode) {
    const int64_t offsets[]{3, 7, -1, 0, 3, 4};
    TargetBitmap candidates(6, false);
    candidates.set(0);
    candidates.set(3);
    candidates.set(4);
    if (backend() == 2) {
        // Footer initialization is complete; reading this excluded file now
        // fails, so success proves it never reaches pin or reader Take.
        ASSERT_TRUE(std::filesystem::remove(dir_ / "4.vx"));
    }
    auto take = column_->Take(nullptr,
                              {OffsetView::From(offsets, 6),
                               TargetType::Json,
                               nullptr,
                               TakePinPolicy::Batch,
                               &candidates});
    ASSERT_NE(take, nullptr);
    EXPECT_EQ(take->Size(), 6);
    for (const auto i : {0, 3, 4}) {
        const auto item = take->Get<Json>(i);
        EXPECT_EQ(item.is_valid, valid(offsets[i]));
        if (item.is_valid) {
            ASSERT_TRUE(item.value);
            EXPECT_EQ(std::string_view(*item.value), values_[offsets[i]]);
        }
    }
    EXPECT_THROW(take->Get<Json>(2), SegcoreError);
    EXPECT_THROW(take->IsValid(1), SegcoreError);
    const int64_t excluded[]{4, 7};
    EXPECT_FALSE(column_->CellsLoaded(excluded, 2));
    const auto owned = take->GetOwn();
    take.reset();
    Evict();
    EXPECT_EQ(std::string_view(owned.values.data_as<Json>()[3]), values_[0]);
}

TEST_P(SealedJsonTakeTest, EmptyCandidateMaskDoesNotReadAnyOffsetOrFile) {
    const int64_t offsets[]{-1, 100};
    TargetBitmap candidates(2, false);
    if (backend() == 2) {
        ASSERT_TRUE(std::filesystem::remove(dir_ / "0.vx"));
        ASSERT_TRUE(std::filesystem::remove(dir_ / "4.vx"));
    }
    auto options = TakeOptions{OffsetView::From(offsets, 2),
                               TargetType::Json,
                               nullptr,
                               TakePinPolicy::Batch,
                               &candidates};
    auto take = column_->Take(nullptr, options);
    ASSERT_NE(take, nullptr);
    EXPECT_EQ(take->GetOwn().size, 2);
    EXPECT_THROW(take->Get<Json>(0), SegcoreError);
    TargetBitmap wrong_size(1, true);
    options.candidate_mask = &wrong_size;
    EXPECT_THROW(column_->Take(nullptr, options), SegcoreError);
}

TEST_P(SealedJsonTakeTest, SegmentCallbackRetainsReaderOwnership) {
    const auto before = TakeCalls();
    const std::vector<int64_t> offsets{7, 3, 4, 0, 7};
    std::vector<Json> escaped(offsets.size());
    size_t visited = 0;
    segment_->BulkGetJsonData(
        nullptr,
        field_,
        [&](Json value, size_t i, bool is_valid) {
            EXPECT_EQ(i, visited++);
            EXPECT_EQ(is_valid, valid(offsets[i]));
            if (is_valid) {
                EXPECT_EQ(std::string_view(value), values_[offsets[i]]);
                if (backend() == 2) {
                    escaped[i] = std::move(value);
                }
            }
        },
        offsets.data(),
        offsets.size());
    EXPECT_EQ(visited, offsets.size());
    if (backend() != 2) {
        EXPECT_EQ(TakeCalls(), before + 1);
    }
    Evict();
    if (backend() == 2) {
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (valid(offsets[i])) {
                EXPECT_EQ(std::string_view(escaped[i]), values_[offsets[i]]);
                EXPECT_NO_THROW((void)escaped[i].doc());
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         SealedJsonTakeTest,
                         testing::Combine(testing::Values(0, 1, 2),
                                          testing::Bool()));

}  // namespace
}  // namespace milvus::segcore
