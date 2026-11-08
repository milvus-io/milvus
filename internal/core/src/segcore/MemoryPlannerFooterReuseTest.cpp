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
#include <atomic>
#include <future>
#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>

#include "milvus-storage/common/constants.h"
#include "segcore/memory_planner.h"
#include "storage/KeyRetriever.h"

namespace milvus::segcore {
namespace {

using namespace milvus_storage;

// Every input gets its own BufferReader. Track footer intersections separately
// from data reads; creating a prepared reader must issue neither kind of read.
struct ReadCounts {
    std::atomic<int> opens{0};
    std::atomic<int> handles{0};
    std::atomic<int> footer_reads{0};
    std::atomic<int> data_reads{0};
    std::atomic<bool> fail_reads{false};
    std::atomic<bool> fail_opens{false};
};

class CountedInput : public arrow::io::RandomAccessFile {
 public:
    CountedInput(std::shared_ptr<arrow::Buffer> data,
                 std::shared_ptr<ReadCounts> counts)
        : input_(std::move(data)), counts_(std::move(counts)) {
        ++counts_->handles;
        auto buffer = input_.buffer();
        if (buffer->size() < 8) {
            footer_start_ = 0;
            return;
        }
        const auto* tail = buffer->data() + buffer->size() - 8;
        uint32_t footer_size = static_cast<uint32_t>(tail[0]) |
                               static_cast<uint32_t>(tail[1]) << 8 |
                               static_cast<uint32_t>(tail[2]) << 16 |
                               static_cast<uint32_t>(tail[3]) << 24;
        footer_start_ = buffer->size() - 8 - footer_size;
    }
    ~CountedInput() override {
        (void)CountedInput::Close();
    }
    arrow::Status
    Close() override {
        if (!input_.closed()) {
            --counts_->handles;
        }
        return input_.Close();
    }
    bool
    closed() const override {
        return input_.closed();
    }
    arrow::Result<int64_t>
    Tell() const override {
        return input_.Tell();
    }
    arrow::Status
    Seek(int64_t position) override {
        return input_.Seek(position);
    }
    arrow::Result<int64_t>
    GetSize() override {
        return input_.GetSize();
    }
    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto pos, Tell());
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_.Read(nbytes, out);
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto pos, Tell());
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_.Read(nbytes);
    }
    arrow::Result<int64_t>
    ReadAt(int64_t pos, int64_t nbytes, void* out) override {
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_.ReadAt(pos, nbytes, out);
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t pos, int64_t nbytes) override {
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_.ReadAt(pos, nbytes);
    }

 private:
    arrow::Status
    Record(int64_t pos, int64_t nbytes) {
        if (counts_->fail_reads) {
            return arrow::Status::IOError("injected read failure");
        }
        if (pos + nbytes > footer_start_) {
            ++counts_->footer_reads;
        } else {
            ++counts_->data_reads;
        }
        return arrow::Status::OK();
    }
    arrow::io::BufferReader input_;
    std::shared_ptr<ReadCounts> counts_;
    int64_t footer_start_;
};

// Serve an immutable packed file through a fresh counted handle for each reader.
class CountedFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    CountedFileSystem(std::shared_ptr<arrow::Buffer> data, std::string key)
        : arrow::fs::SubTreeFileSystem(
              "", std::make_shared<arrow::fs::LocalFileSystem>()),
          counts(std::make_shared<ReadCounts>()),
          data_(std::move(data)),
          key_(std::move(key)) {
    }
    using arrow::fs::SubTreeFileSystem::OpenInputFile;
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        if (counts->fail_opens || path != key_) {
            return arrow::Status::IOError("missing file: ", path);
        }
        ++counts->opens;
        return std::make_shared<CountedInput>(data_, counts);
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const arrow::fs::FileInfo& info) override {
        return OpenInputFile(info.path());
    }
    std::shared_ptr<ReadCounts> counts;

 private:
    std::shared_ptr<arrow::Buffer> data_;
    std::string key_;
};

const std::string kKey = "footer-reuse.parquet";

arrow::Result<std::shared_ptr<arrow::Buffer>>
MakePackedFile(int32_t base = 0) {
    RowGroupMetadataVector groups;
    for (int i = 0; i < 3; ++i) {
        groups.Add(RowGroupMetadata(8, 2, 2 * i));
    }
    GroupFieldIDList fields(std::vector<std::vector<int>>{{100}});
    auto kv = arrow::key_value_metadata(
        {ROW_GROUP_META_KEY,
         milvus_storage::STORAGE_VERSION_KEY,
         GROUP_FIELD_ID_LIST_META_KEY},
        {groups.Serialize(), "test", fields.Serialize()});
    auto schema = arrow::schema(
        {arrow::field(
            "value",
            arrow::int32(),
            false,
            arrow::key_value_metadata({ARROW_FIELD_ID_KEY}, {"100"}))},
        kv);
    arrow::Int32Builder values;
    ARROW_RETURN_NOT_OK(values.AppendValues(
        {base, base + 1, base + 2, base + 3, base + 4, base + 5}));
    ARROW_ASSIGN_OR_RAISE(auto array, values.Finish());
    auto table = arrow::Table::Make(schema, {array});
    ARROW_ASSIGN_OR_RAISE(auto output, arrow::io::BufferOutputStream::Create());
    parquet::WriterProperties::Builder props;
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        output,
        2,
        props.build(),
        parquet::ArrowWriterProperties::Builder().store_schema()->build()));
    return output->Finish();
}

class MemoryPlannerFooterReuseTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        auto data = MakePackedFile();
        ASSERT_TRUE(data.ok()) << data.status();
        fs_ = std::make_shared<CountedFileSystem>(data.ValueOrDie(), kKey);
        auto result = PreparedParquetFile::Open(
            fs_, kKey, storage::GetReaderProperties());
        ASSERT_TRUE(result.ok()) << result.status();
        prepared_ = std::move(result).ValueOrDie();
        footer_reads_ = fs_->counts->footer_reads.load();
        ASSERT_GT(footer_reads_, 0);
        ASSERT_EQ(fs_->counts->handles, 0);
    }

    void
    ExpectTables(const std::vector<std::shared_ptr<arrow::Table>>& tables,
                 int group) {
        ASSERT_EQ(tables.size(), 1);
        ASSERT_EQ(tables[0]->num_rows(), 2);
        EXPECT_EQ(std::static_pointer_cast<arrow::Int32Array>(
                      tables[0]->column(0)->chunk(0))
                      ->Value(0),
                  2 * group);
    }

    std::shared_ptr<CountedFileSystem> fs_;
    std::shared_ptr<PreparedParquetFile> prepared_;
    int footer_reads_ = 0;
};

TEST_F(MemoryPlannerFooterReuseTest,
       FactorySharesFooterAcrossConcurrentBatches) {
    auto factory = MakeFileReaderFactory({prepared_});
    std::weak_ptr<PreparedParquetFile> owner = prepared_;
    prepared_.reset();
    std::promise<void> start;
    auto ready = start.get_future().share();
    std::vector<
        std::future<arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>>>
        tasks;
    for (int i = 0; i < 12; ++i) {
        tasks.push_back(std::async(std::launch::async, [factory, ready, i] {
            ready.wait();
            return factory(0, i % 3, 1, 8);
        }));
    }
    factory = {};
    EXPECT_FALSE(owner.expired());
    start.set_value();
    for (int i = 0; i < tasks.size(); ++i) {
        auto result = tasks[i].get();
        ASSERT_TRUE(result.ok()) << result.status();
        ExpectTables(result.ValueOrDie(), i % 3);
    }
    tasks.clear();
    EXPECT_TRUE(owner.expired());
    EXPECT_EQ(fs_->counts->footer_reads, footer_reads_);
    EXPECT_GT(fs_->counts->data_reads, 0);
    EXPECT_EQ(fs_->counts->handles, 0);
}

TEST_F(MemoryPlannerFooterReuseTest, StrategyReusesMetadataProbeAcrossCalls) {
    std::weak_ptr<PreparedParquetFile> owner = prepared_;
    PreparedParquetFiles files{std::move(prepared_)};
    for (int group = 0; group < 3; ++group) {
        auto channel = std::make_shared<ArrowReaderChannel>(2);
        LoadWithStrategy({kKey},
                         channel,
                         1024,
                         std::make_unique<ParallelDegreeSplitStrategy>(1),
                         {{group}},
                         fs_,
                         nullptr,
                         proto::common::LoadPriority::HIGH,
                         files);
        std::shared_ptr<ArrowDataWrapper> wrapper;
        ASSERT_TRUE(channel->pop(wrapper));
        ASSERT_EQ(wrapper->arrow_tables.size(), 1);
        EXPECT_EQ(wrapper->arrow_tables[0].row_group_index, group);
        ExpectTables({wrapper->arrow_tables[0].table}, group);
        EXPECT_FALSE(channel->pop(wrapper));
    }
    EXPECT_EQ(fs_->counts->footer_reads, footer_reads_);
    EXPECT_EQ(fs_->counts->handles, 0);
    files.clear();
    EXPECT_TRUE(owner.expired());
}

TEST_F(MemoryPlannerFooterReuseTest, FactoryFailuresKeepContextReusable) {
    auto factory = MakeFileReaderFactory({prepared_});
    fs_->counts->fail_opens = true;
    auto failed_open = factory(0, 0, 1, 8);
    ASSERT_FALSE(failed_open.ok());
    EXPECT_TRUE(failed_open.status().IsIOError());
    fs_->counts->fail_opens = false;
    fs_->counts->fail_reads = true;
    auto failed_read = factory(0, 0, 1, 8);
    ASSERT_FALSE(failed_read.ok());
    EXPECT_TRUE(failed_read.status().IsIOError());
    EXPECT_EQ(fs_->counts->handles, 0);
    fs_->counts->fail_reads = false;
    auto recovered = factory(0, 2, 1, 8);
    ASSERT_TRUE(recovered.ok()) << recovered.status();
    ExpectTables(recovered.ValueOrDie(), 2);
    EXPECT_EQ(fs_->counts->footer_reads, footer_reads_);
    EXPECT_EQ(fs_->counts->handles, 0);
}

}  // namespace
}  // namespace milvus::segcore
