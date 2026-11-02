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

#include <gtest/gtest.h>
#include <arrow/filesystem/localfs.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/OpContext.h"
#include "index/Meta.h"
#include "segcore/Types.h"
#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "storage/Util.h"

namespace milvus::segcore::storagev1translator {
namespace {

class CancellingMetadataFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    explicit CancellingMetadataFileSystem(folly::CancellationSource& cancelled)
        : SubTreeFileSystem("", std::make_shared<arrow::fs::LocalFileSystem>()),
          cancelled_(cancelled) {
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string&) override {
        ++opens;
        cancelled_.requestCancellation();
        return arrow::Status::IOError("injected metadata open failure");
    }

    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(OpenInputFile(path));
    }

    size_t opens{0};

 private:
    folly::CancellationSource& cancelled_;
};

class MetadataOpenCancellationTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = std::filesystem::temp_directory_path().string();
        fs_ = std::make_shared<CancellingMetadataFileSystem>(cancelled_);
        context_ =
            storage::FileManagerContext(storage::FieldDataMeta{1, 2, 3, 101},
                                        storage::IndexMeta{3, 101, 1000, 10000},
                                        storage::CreateChunkManager(config),
                                        fs_);
        context_.set_for_loading_index(true);
    }

    folly::CancellationSource cancelled_;
    std::shared_ptr<CancellingMetadataFileSystem> fs_;
    storage::FileManagerContext context_;
};

TEST_F(MetadataOpenCancellationTest, SealedSyncFailedOpenReportsCancellation) {
    context_.use_async_load = false;
    context_.fieldDataMeta.field_schema.set_data_type(
        proto::schema::DataType::Int32);
    LoadIndexInfo info{};
    info.segment_id = 3;
    info.field_id = 101;
    info.field_type = DataType::INT32;
    info.element_type = DataType::NONE;
    info.index_engine_version = 3;
    info.index_params = {{index::INDEX_TYPE, index::ASCENDING_SORT},
                         {index::SCALAR_INDEX_ENGINE_VERSION, "3"}};
    info.index_files = {"metadata.v3"};
    info.num_rows = 1;
    info.warmup_policy = "disable";
    OpContext operation(cancelled_.getToken());
    try {
        SealedIndexTranslator translator(&info,
                                         tracer::TraceContext{},
                                         context_,
                                         Config(info.index_params),
                                         &operation);
        FAIL() << "cancelled failed open must report cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(fs_->opens, 1);
}

TEST_F(MetadataOpenCancellationTest, TextAsyncFailedOpenReportsCancellation) {
    context_.use_async_load = true;
    context_.fieldDataMeta.field_schema.set_data_type(
        proto::schema::DataType::VarChar);
    const TextMatchIndexLoadInfo info{
        false, 3, 101, "{}", 4096, 1, "disable", ""};
    Config config{
        {index::INDEX_FILES, std::vector<std::string>{"metadata.v3"}}};
    OpContext operation(cancelled_.getToken());
    try {
        TextMatchIndexTranslator translator(info, context_, config, &operation);
        FAIL() << "cancelled failed open must report cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(fs_->opens, 1);
}

}  // namespace
}  // namespace milvus::segcore::storagev1translator
