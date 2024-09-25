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
class ChunkCacheTest : public testing::TestWithParam</*mmap enabled*/ bool> {
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
    const char* dense_file_name = "chunk_cache_test/insert_log/2/101/1000000";
    const char* sparse_file_name = "chunk_cache_test/insert_log/2/102/1000000";
    milvus::storage::MmapChunkManagerPtr mcm;
    milvus::segcore::SegcoreConfig config;
    milvus::storage::MmapChunkDescriptorPtr descriptor =
        std::shared_ptr<milvus::storage::MmapChunkDescriptor>(
            new milvus::storage::MmapChunkDescriptor(
                {101, SegmentType::Sealed}));
};

INSTANTIATE_TEST_SUITE_P(ChunkCacheTestSuite,
                         ChunkCacheTest,
                         testing::Values(true, false));

TEST_P(ChunkCacheTest, Read) {
    auto N = 10000;
    auto dim = 128;
    auto dense_metric_type = knowhere::metric::L2;
    auto sparse_metric_type = knowhere::metric::IP;

    auto schema = std::make_shared<milvus::Schema>();
    auto fake_dense_vec_id = schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, dim, dense_metric_type);
    auto i64_fid = schema->AddDebugField("counter", milvus::DataType::INT64);
    auto fake_sparse_vec_id =
        schema->AddDebugField("fakevec_sparse",
                              milvus::DataType::VECTOR_SPARSE_FLOAT,
                              dim,
                              sparse_metric_type);
    schema->set_primary_field_id(i64_fid);

    auto dataset = milvus::segcore::DataGen(schema, N);

    auto dense_field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_dense_vec_id.get()};
    auto sparse_field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_sparse_vec_id.get()};
    auto dense_field_meta = milvus::FieldMeta(milvus::FieldName("fakevec"),
                                              fake_dense_vec_id,
                                              milvus::DataType::VECTOR_FLOAT,
                                              dim,
                                              dense_metric_type,
                                              false);
    auto sparse_field_meta =
        milvus::FieldMeta(milvus::FieldName("fakevec_sparse"),
                          fake_sparse_vec_id,
                          milvus::DataType::VECTOR_SPARSE_FLOAT,
                          dim,
                          sparse_metric_type,
                          false);

    auto lcm = milvus::storage::LocalChunkManagerSingleton::GetInstance()
                   .GetChunkManager();
    auto dense_data = dataset.get_col<float>(fake_dense_vec_id);
    auto sparse_data =
        dataset.get_col<knowhere::sparse::SparseRow<float>>(fake_sparse_vec_id);

    auto data_slices = std::vector<void*>{dense_data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{dense_file_name};
    PutFieldData(lcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 dense_field_data_meta,
                 dense_field_meta);

    data_slices = std::vector<void*>{sparse_data.data()};
    slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    slice_names = std::vector<std::string>{sparse_file_name};
    PutFieldData(lcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 sparse_field_data_meta,
                 sparse_field_meta);

    auto cc = milvus::storage::MmapManager::GetInstance().GetChunkCache();

    // validate dense data
    const auto& dense_column =
        cc->Read(dense_file_name, descriptor, dense_field_meta, GetParam());
    Assert(dense_column->DataByteSize() == dim * N * 4);
    auto actual_dense = (const float*)(dense_column->Data());
    for (auto i = 0; i < N * dim; i++) {
        AssertInfo(dense_data[i] == actual_dense[i],
                   fmt::format(
                       "expect {}, actual {}", dense_data[i], actual_dense[i]));
    }

    // validate sparse data
    const auto& sparse_column =
        cc->Read(sparse_file_name, descriptor, sparse_field_meta, GetParam());
    auto expected_sparse_size = 0;
    auto actual_sparse =
        (const knowhere::sparse::SparseRow<float>*)(sparse_column->Data());
    for (auto i = 0; i < N; i++) {
        const auto& actual_sparse_row = actual_sparse[i];
        const auto& expect_sparse_row = sparse_data[i];
        AssertInfo(
            actual_sparse_row.size() == expect_sparse_row.size(),
            fmt::format("Incorrect size of sparse row: expect {}, actual {}",
                        expect_sparse_row.size(),
                        actual_sparse_row.size()));
        auto bytes = actual_sparse_row.data_byte_size();
        AssertInfo(
            memcmp(actual_sparse_row.data(), expect_sparse_row.data(), bytes) ==
                0,
            fmt::format("Incorrect data of sparse row: expect {}, actual {}",
                        expect_sparse_row.data(),
                        actual_sparse_row.data()));
        expected_sparse_size += bytes;
    }

    ASSERT_EQ(sparse_column->DataByteSize(), expected_sparse_size);

    cc->Remove(dense_file_name);
    cc->Remove(sparse_file_name);
    lcm->Remove(dense_file_name);
    lcm->Remove(sparse_file_name);
}

TEST_P(ChunkCacheTest, TestMultithreads) {
    auto N = 1000;
    auto dim = 128;
    auto dense_metric_type = knowhere::metric::L2;
    auto sparse_metric_type = knowhere::metric::IP;

    auto schema = std::make_shared<milvus::Schema>();
    auto fake_dense_vec_id = schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, dim, dense_metric_type);
    auto fake_sparse_vec_id =
        schema->AddDebugField("fakevec_sparse",
                              milvus::DataType::VECTOR_SPARSE_FLOAT,
                              dim,
                              sparse_metric_type);
    auto i64_fid = schema->AddDebugField("counter", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto dataset = milvus::segcore::DataGen(schema, N);

    auto dense_field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_dense_vec_id.get()};
    auto sparse_field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fake_sparse_vec_id.get()};
    auto dense_field_meta = milvus::FieldMeta(milvus::FieldName("fakevec"),
                                              fake_dense_vec_id,
                                              milvus::DataType::VECTOR_FLOAT,
                                              dim,
                                              dense_metric_type,
                                              false);
    auto sparse_field_meta =
        milvus::FieldMeta(milvus::FieldName("fakevec_sparse"),
                          fake_sparse_vec_id,
                          milvus::DataType::VECTOR_SPARSE_FLOAT,
                          dim,
                          sparse_metric_type,
                          false);

    auto lcm = milvus::storage::LocalChunkManagerSingleton::GetInstance()
                   .GetChunkManager();
    auto dense_data = dataset.get_col<float>(fake_dense_vec_id);
    auto sparse_data =
        dataset.get_col<knowhere::sparse::SparseRow<float>>(fake_sparse_vec_id);

    auto dense_data_slices = std::vector<void*>{dense_data.data()};
    auto sparse_data_slices = std::vector<void*>{sparse_data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto dense_slice_names = std::vector<std::string>{dense_file_name};
    auto sparse_slice_names = std::vector<std::string>{sparse_file_name};

    PutFieldData(lcm.get(),
                 dense_data_slices,
                 slice_sizes,
                 dense_slice_names,
                 dense_field_data_meta,
                 dense_field_meta);

    PutFieldData(lcm.get(),
                 sparse_data_slices,
                 slice_sizes,
                 sparse_slice_names,
                 sparse_field_data_meta,
                 sparse_field_meta);

    auto cc = milvus::storage::MmapManager::GetInstance().GetChunkCache();

    constexpr int threads = 16;
    std::vector<int64_t> total_counts(threads);
    auto executor = [&](int thread_id) {
        const auto& dense_column =
            cc->Read(dense_file_name, descriptor, dense_field_meta, GetParam());
        Assert(dense_column->DataByteSize() == dim * N * 4);

        auto actual_dense = (const float*)dense_column->Data();
        for (auto i = 0; i < N * dim; i++) {
            AssertInfo(
                dense_data[i] == actual_dense[i],
                fmt::format(
                    "expect {}, actual {}", dense_data[i], actual_dense[i]));
        }

        const auto& sparse_column = cc->Read(
            sparse_file_name, descriptor, sparse_field_meta, GetParam());
        auto actual_sparse =
            (const knowhere::sparse::SparseRow<float>*)sparse_column->Data();
        for (auto i = 0; i < N; i++) {
            const auto& actual_sparse_row = actual_sparse[i];
            const auto& expect_sparse_row = sparse_data[i];
            AssertInfo(actual_sparse_row.size() == expect_sparse_row.size(),
                       fmt::format(
                           "Incorrect size of sparse row: expect {}, actual {}",
                           expect_sparse_row.size(),
                           actual_sparse_row.size()));
            auto bytes = actual_sparse_row.data_byte_size();
            AssertInfo(memcmp(actual_sparse_row.data(),
                              expect_sparse_row.data(),
                              bytes) == 0,
                       fmt::format(
                           "Incorrect data of sparse row: expect {}, actual {}",
                           expect_sparse_row.data(),
                           actual_sparse_row.data()));
        }
    };
    std::vector<std::thread> pool;
    for (int i = 0; i < threads; ++i) {
        pool.emplace_back(executor, i);
    }
    for (auto& thread : pool) {
        thread.join();
    }

    cc->Remove(dense_file_name);
    cc->Remove(sparse_file_name);
    lcm->Remove(dense_file_name);
    lcm->Remove(sparse_file_name);
}
