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

#include <string>
#include <vector>

#include "fmt/format.h"
#include "common/Schema.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "storage/ChunkCache.h"
#include "storage/LocalChunkManagerSingleton.h"

#define DEFAULT_READ_AHEAD_POLICY "willneed"
class ChunkCacheTest : public testing::Test {
 public:
    void
    SetUp() override {
        mcm = milvus::storage::MmapManager::GetInstance().GetMmapChunkManager();
        mcm->Register(descriptor);
    }
    void
    TearDown() override {
        mcm->UnRegister(descriptor);
    }
    const char* file_name = "chunk_cache_test/insert_log/2/101/1000000";
    milvus::storage::MmapChunkManagerPtr mcm;
    milvus::segcore::SegcoreConfig config;
    milvus::storage::MmapChunkDescriptorPtr descriptor =
        std::shared_ptr<milvus::storage::MmapChunkDescriptor>(
            new milvus::storage::MmapChunkDescriptor(
                {101, SegmentType::Sealed}));
};

TEST_F(ChunkCacheTest, Read) {
    auto N = 10000;
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;

    auto schema = std::make_shared<milvus::Schema>();
    auto fake_id = schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto dataset = milvus::segcore::DataGen(schema, N);

    auto field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_id.get()};
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"),
                                        fake_id,
                                        milvus::DataType::VECTOR_FLOAT,
                                        dim,
                                        metric_type);

    auto lcm = milvus::storage::LocalChunkManagerSingleton::GetInstance()
                   .GetChunkManager();
    auto data = dataset.get_col<float>(fake_id);
    auto data_slices = std::vector<void*>{data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(lcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 field_data_meta,
                 field_meta);

    auto cc = milvus::storage::MmapManager::GetInstance().GetChunkCache();
    const auto& column = cc->Read(file_name, descriptor, field_meta, true);
    Assert(column->ByteSize() == dim * N * 4);

    auto actual = (float*)column->Data();
    for (auto i = 0; i < N; i++) {
        AssertInfo(data[i] == actual[i],
                   fmt::format("expect {}, actual {}", data[i], actual[i]));
    }

    cc->Remove(file_name);
    lcm->Remove(file_name);
}

TEST_F(ChunkCacheTest, ReadByMemoryMode) {
    auto N = 10000;
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;

    auto schema = std::make_shared<milvus::Schema>();
    auto fake_id = schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto dataset = milvus::segcore::DataGen(schema, N);

    auto field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_id.get()};
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"),
                                        fake_id,
                                        milvus::DataType::VECTOR_FLOAT,
                                        dim,
                                        metric_type);

    auto lcm = milvus::storage::LocalChunkManagerSingleton::GetInstance()
                   .GetChunkManager();
    auto data = dataset.get_col<float>(fake_id);
    auto data_slices = std::vector<void*>{data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(lcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 field_data_meta,
                 field_meta);

    auto cc = milvus::storage::MmapManager::GetInstance().GetChunkCache();
    const auto& column = cc->Read(file_name, descriptor, field_meta, false);
    Assert(column->ByteSize() == dim * N * 4);

    auto actual = (float*)column->Data();
    for (auto i = 0; i < N; i++) {
        AssertInfo(data[i] == actual[i],
                   fmt::format("expect {}, actual {}", data[i], actual[i]));
    }

    cc->Remove(file_name);
    lcm->Remove(file_name);
}

TEST_F(ChunkCacheTest, TestMultithreads) {
    auto N = 1000;
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;

    auto schema = std::make_shared<milvus::Schema>();
    auto fake_id = schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto dataset = milvus::segcore::DataGen(schema, N);

    auto field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_id.get()};
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"),
                                        fake_id,
                                        milvus::DataType::VECTOR_FLOAT,
                                        dim,
                                        metric_type);

    auto lcm = milvus::storage::LocalChunkManagerSingleton::GetInstance()
                   .GetChunkManager();
    auto data = dataset.get_col<float>(fake_id);
    auto data_slices = std::vector<void*>{data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(lcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 field_data_meta,
                 field_meta);

    auto cc = milvus::storage::MmapManager::GetInstance().GetChunkCache();

    constexpr int threads = 16;
    std::vector<int64_t> total_counts(threads);
    auto executor = [&](int thread_id) {
        const auto& column = cc->Read(file_name, descriptor, field_meta, true);
        Assert(column->ByteSize() == dim * N * 4);

        auto actual = (float*)column->Data();
        for (auto i = 0; i < N; i++) {
            AssertInfo(data[i] == actual[i],
                       fmt::format("expect {}, actual {}", data[i], actual[i]));
        }
    };
    std::vector<std::thread> pool;
    for (int i = 0; i < threads; ++i) {
        pool.emplace_back(executor, i);
    }
    for (auto& thread : pool) {
        thread.join();
    }

    cc->Remove(file_name);
    lcm->Remove(file_name);
}
