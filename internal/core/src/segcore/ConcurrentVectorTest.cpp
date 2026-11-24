// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <assert.h>
#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <random>
#include <thread>
#include <tuple>
#include <vector>

#include "common/EasyAssert.h"
#include "common/OffsetMapping.h"
#include "gtest/gtest.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/SegcoreConfig.h"
#include "storage/Util.h"

using namespace milvus::segcore;
using std::vector;

namespace {

// A nullable INT64 batch. Only the validity bits are under test here; the
// values are filler.
milvus::FieldDataPtr
MakeNullableInt64Batch(const std::vector<bool>& valid) {
    const auto num_rows = static_cast<int64_t>(valid.size());
    std::vector<int64_t> values(num_rows, 0);
    std::vector<uint8_t> bitmap((num_rows + 7) / 8, 0);
    for (int64_t i = 0; i < num_rows; ++i) {
        if (valid[i]) {
            bitmap[i / 8] |= (1 << (i % 8));
        }
    }
    auto field_data = milvus::storage::CreateFieldData(
        milvus::DataType::INT64, milvus::DataType::NONE, true, 1, num_rows);
    field_data->FillFieldData(values.data(), bitmap.data(), num_rows, 0);
    return field_data;
}

std::vector<bool>
BitsOf(const ThreadSafeValidData& valid_data) {
    auto bits = valid_data.get_data();
    return std::vector<bool>(bits.begin(), bits.end());
}

}  // namespace

TEST(ConcurrentVector, TestSingle) {
    auto dim = 8;
    ConcurrentVectorImpl<int, false> c_vec(dim, 32);
    std::default_random_engine e(42);
    int data = 0;
    auto total_count = 0;
    for (int i = 0; i < 10000; ++i) {
        int insert_size = e() % 150;
        vector<int> vec(insert_size * dim);
        for (auto& x : vec) {
            x = data++;
        }
        c_vec.set_data_raw(total_count, vec.data(), insert_size);
        total_count += insert_size;
    }
    ASSERT_EQ(c_vec.num_chunk(), (total_count + 31) / 32);
    for (int i = 0; i < total_count; ++i) {
        for (int d = 0; d < dim; ++d) {
            auto std_data = d + i * dim;
            ASSERT_EQ(c_vec.get_element(i)[d], std_data);
        }
    }
}

TEST(ConcurrentVector, JsonInsertAcrossChunks) {
    milvus::FieldMeta field_meta(milvus::FieldName("json"),
                                 milvus::FieldId(101),
                                 milvus::DataType::JSON,
                                 false,
                                 std::nullopt);
    std::vector<std::string> rows;
    for (int i = 0; i < 10; ++i) {
        std::string row = "{\"id\":";
        row.append(std::to_string(i));
        row.append(",\"name\":\"");
        row.append(i * 10, 'x');
        row.append("\"}");
        rows.push_back(std::move(row));
    }
    milvus::DataArray first;
    milvus::DataArray second;
    for (int i = 0; i < 10; ++i) {
        auto& batch = i < 3 ? first : second;
        batch.mutable_scalars()->mutable_json_data()->add_data(rows[i]);
    }

    ConcurrentVector<milvus::Json> c_vec(4);
    // The DataArray overload is declared on VectorBase and hidden by the
    // set_data_raw overloads of ConcurrentVectorImpl.
    VectorBase& base = c_vec;
    base.set_data_raw(0, 3, &first, field_meta);
    base.set_data_raw(3, 7, &second, field_meta);

    ASSERT_EQ(c_vec.num_chunk(), 3);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(c_vec.view_element(i), rows[i]);
    }
}

// VARCHAR and GEOMETRY rows are moved into the chunk like JSON rows. The rows
// are longer than the small-string buffer, so a move hands over the heap
// allocation.
TEST(ConcurrentVector, StringInsertAcrossChunks) {
    milvus::FieldMeta field_meta(milvus::FieldName("varchar"),
                                 milvus::FieldId(101),
                                 milvus::DataType::VARCHAR,
                                 65535,
                                 false,
                                 std::nullopt);
    std::vector<std::string> rows;
    for (int i = 0; i < 10; ++i) {
        rows.push_back(std::to_string(i) + std::string(40 + i * 10, 'x'));
    }
    milvus::DataArray first;
    milvus::DataArray second;
    for (int i = 0; i < 10; ++i) {
        auto& batch = i < 3 ? first : second;
        batch.mutable_scalars()->mutable_string_data()->add_data(rows[i]);
    }

    ConcurrentVector<std::string> c_vec(4);
    VectorBase& base = c_vec;
    base.set_data_raw(0, 3, &first, field_meta);
    base.set_data_raw(3, 7, &second, field_meta);

    ASSERT_EQ(c_vec.num_chunk(), 3);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(c_vec.view_element(i), rows[i]);
    }
    // The payload is only read.
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(first.scalars().string_data().data(i), rows[i]);
    }
}

TEST(ConcurrentVector, GeometryInsertAcrossChunks) {
    milvus::FieldMeta field_meta(milvus::FieldName("geometry"),
                                 milvus::FieldId(101),
                                 milvus::DataType::GEOMETRY,
                                 false,
                                 std::nullopt);
    std::vector<std::string> rows;
    for (int i = 0; i < 10; ++i) {
        rows.push_back(std::to_string(i) + std::string(40 + i * 10, 'g'));
    }
    milvus::DataArray first;
    milvus::DataArray second;
    for (int i = 0; i < 10; ++i) {
        auto& batch = i < 3 ? first : second;
        batch.mutable_scalars()->mutable_geometry_data()->add_data(rows[i]);
    }

    ConcurrentVector<std::string> c_vec(4);
    VectorBase& base = c_vec;
    base.set_data_raw(0, 3, &first, field_meta);
    base.set_data_raw(3, 7, &second, field_meta);

    ASSERT_EQ(c_vec.num_chunk(), 3);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(c_vec.view_element(i), rows[i]);
    }
}

// A payload with fewer rows than the count to write is rejected before any row
// is moved.
TEST(ConcurrentVector, ShortStringPayloadIsRejected) {
    milvus::FieldMeta field_meta(milvus::FieldName("varchar"),
                                 milvus::FieldId(101),
                                 milvus::DataType::VARCHAR,
                                 65535,
                                 false,
                                 std::nullopt);
    milvus::DataArray data;
    data.mutable_scalars()->mutable_string_data()->add_data("only row");

    ConcurrentVector<std::string> c_vec(4);
    VectorBase& base = c_vec;
    ASSERT_ANY_THROW(base.set_data_raw(0, 2, &data, field_meta));
}

// A payload with more rows than the count to write is as misaligned, so it is
// rejected rather than cut to the count.
TEST(ConcurrentVector, LongStringPayloadIsRejected) {
    milvus::FieldMeta field_meta(milvus::FieldName("varchar"),
                                 milvus::FieldId(101),
                                 milvus::DataType::VARCHAR,
                                 65535,
                                 false,
                                 std::nullopt);
    milvus::DataArray data;
    data.mutable_scalars()->mutable_string_data()->add_data("first row");
    data.mutable_scalars()->mutable_string_data()->add_data("second row");

    ConcurrentVector<std::string> c_vec(4);
    VectorBase& base = c_vec;
    ASSERT_ANY_THROW(base.set_data_raw(0, 1, &data, field_meta));
}

TEST(ConcurrentVector, TestMultithreads) {
    auto dim = 8;
    constexpr int threads = 16;
    std::vector<int64_t> total_counts(threads);

    ConcurrentVectorImpl<int64_t, false> c_vec(dim, 32);
    std::atomic<int64_t> ack_counter = 0;

    auto executor = [&](int thread_id) {
        std::default_random_engine e(42 + thread_id);
        int64_t data = 0;
        int64_t total_count = 0;
        for (int i = 0; i < 2000; ++i) {
            int insert_size = e() % 150;
            vector<int64_t> vec(insert_size * dim);
            for (auto& x : vec) {
                x = data++ * threads + thread_id;
            }
            auto offset = ack_counter.fetch_add(insert_size);
            c_vec.set_data_raw(offset, vec.data(), insert_size);
            total_count += insert_size;
        }
        assert(data == total_count * dim);
        total_counts[thread_id] = total_count;
    };
    std::vector<std::thread> pool;
    for (int i = 0; i < threads; ++i) {
        pool.emplace_back(executor, i);
    }
    for (auto& thread : pool) {
        thread.join();
    }

    std::vector<int64_t> counts(threads);
    auto N = ack_counter.load();
    for (int64_t i = 0; i < N; ++i) {
        for (int d = 0; d < dim; ++d) {
            auto data = c_vec.get_element(i)[d];
            auto thread_id = data % threads;
            auto raw_data = data / threads;
            auto std_data = counts[thread_id]++;
            ASSERT_EQ(raw_data, std_data) << data;
        }
    }
}

TEST(ConcurrentVector, TestAckSingle) {
    std::vector<std::tuple<int64_t, int64_t, int64_t>> raw_data;
    std::default_random_engine e(42);
    AckResponder ack;
    int N = 10000;
    for (int i = 0; i < 10000; ++i) {
        auto weight = i + e() % 100;
        raw_data.emplace_back(weight, i, (i + 1));
    }
    std::sort(raw_data.begin(), raw_data.end());
    for (auto [_, b, e] : raw_data) {
        EXPECT_LE(ack.GetAck(), b);
        ack.AddSegment(b, e);
        auto seg = ack.GetAck();
        EXPECT_GE(seg + 100, b);
    }
    EXPECT_EQ(ack.GetAck(), N);
}

// The growing load path reserves a logical range per batch and writes column
// data and the interim index at that offset; validity has to land in the same
// range. A write that would leave a gap has to fail loudly rather than publish
// rows nobody wrote -- is_valid() admits every offset below length_, so those
// bits would read back as garbage, not as null.
TEST(ThreadSafeValidData, WritesFieldDataAtReservedOffset) {
    // size_per_chunk 3 makes the batch written at offset 2 straddle a chunk
    // boundary.
    ThreadSafeValidData valid_data(/*size_per_chunk=*/3);
    EXPECT_TRUE(valid_data.empty());

    const std::vector<bool> a = {true, false};
    const std::vector<bool> b = {false, true};

    valid_data.set_data_raw(0, {MakeNullableInt64Batch(a)});
    valid_data.set_data_raw(2, {MakeNullableInt64Batch(b)});
    EXPECT_EQ(BitsOf(valid_data),
              (std::vector<bool>{true, false, false, true}));

    // A batch carrying several FieldDatas fills the reserved range in order --
    // the write_offset accumulation this overload does and its DataArray
    // sibling does not.
    valid_data.set_data_raw(
        4, {MakeNullableInt64Batch(a), MakeNullableInt64Batch(b)});
    EXPECT_EQ(BitsOf(valid_data),
              (std::vector<bool>{
                  true, false, false, true, true, false, false, true}));

    // Rewriting an already written range is not rejected and does not extend
    // the bitmap, matching the DataArray overload -- a Reopen backfill retry
    // stays idempotent.
    valid_data.set_data_raw(0, {MakeNullableInt64Batch(b)});
    EXPECT_EQ(BitsOf(valid_data),
              (std::vector<bool>{
                  false, true, false, true, true, false, false, true}));

    // Writing at exactly length_ extends the bitmap; starting one row past it
    // would leave a hole and must throw instead.
    ASSERT_EQ(BitsOf(valid_data).size(), 8u);
    EXPECT_NO_THROW(valid_data.set_data_raw(8, {MakeNullableInt64Batch(a)}));
    EXPECT_EQ(BitsOf(valid_data).size(), 10u);
    EXPECT_THROW(valid_data.set_data_raw(11, {MakeNullableInt64Batch(a)}),
                 milvus::SegcoreError);
    EXPECT_EQ(BitsOf(valid_data).size(), 10u);
}
