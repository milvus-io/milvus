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

#include <folly/CancellationToken.h>
#include <folly/FBVector.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/OffsetMapping.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/schema.pb.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/DeletedRecord.h"
#include "segcore/InsertRecord.h"
#include "segcore/Record.h"
#include "segcore/Utils.h"

TEST(Util_Segcore, UpperBound) {
    using milvus::Timestamp;
    using milvus::segcore::ConcurrentVector;
    using milvus::segcore::upper_bound;

    std::vector<Timestamp> data{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ConcurrentVector<Timestamp> timestamps(1);
    timestamps.set_data_raw(0, data.data(), data.size());

    ASSERT_EQ(1, upper_bound(timestamps, 0, data.size(), 0));
    ASSERT_EQ(5, upper_bound(timestamps, 0, data.size(), 4));
    ASSERT_EQ(10, upper_bound(timestamps, 0, data.size(), 10));
}

TEST(Util_Segcore, GetDeleteBitmap) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    (void)vec_fid;
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 10;
    InsertRecord<false> insert_record(*schema, N);
    DeletedRecord<false> delete_record(
        &insert_record,
        [&insert_record](
            const std::vector<PkType>& pks,
            const Timestamp* timestamps,
            std::function<void(const SegOffset offset, const Timestamp ts)>
                cb) {
            for (size_t i = 0; i < pks.size(); ++i) {
                auto timestamp = timestamps[i];
                auto offsets = insert_record.search_pk(pks[i], timestamp);
                for (auto offset : offsets) {
                    cb(offset, timestamp);
                }
            }
        },
        0);

    // fill insert record, all insert records has same pk = 1, timestamps= {1 ... N}
    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = 1;
        tss[i] = i + 1;
        insert_record.insert_pk(1, i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // test case delete pk1(ts = 0) -> insert repeated pk1 (ts = {1 ... N}) -> query (ts = N)
    std::vector<Timestamp> delete_ts = {0};
    std::vector<PkType> delete_pk = {1};
    delete_record.StreamPush(delete_pk, delete_ts.data());

    auto query_timestamp = tss[N - 1];
    auto insert_barrier = get_barrier(insert_record, query_timestamp);
    BitsetType res_bitmap(insert_barrier);
    BitsetTypeView res_view(res_bitmap);
    delete_record.Query(res_view, insert_barrier, query_timestamp);
    ASSERT_EQ(res_view.count(), 0);
}

TEST(Util_Segcore, CreateVectorDataArrayFromNullableVectors) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    auto& field_meta = (*schema)[vec];

    int64_t dim = 16;
    int64_t total_count = 10;
    int64_t valid_count = 5;

    std::vector<float> data(valid_count * dim);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<float>(i);
    }

    std::unique_ptr<bool[]> valid_flags = std::make_unique<bool[]>(total_count);
    for (int64_t i = 0; i < total_count; ++i) {
        if (i % 2 == 0) {
            valid_flags[i] = true;
        } else {
            valid_flags[i] = false;
        }
    }

    auto result = CreateVectorDataArrayFrom(
        data.data(), valid_flags.get(), total_count, valid_count, field_meta);

    ASSERT_TRUE(result->valid_data().size() > 0);
    ASSERT_EQ(result->valid_data().size(), total_count);
    ASSERT_EQ(result->vectors().float_vector().data_size(), valid_count * dim);
}

TEST(Util_Segcore, MergeDataArrayWithNullableVectors) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    auto& field_meta = (*schema)[vec];

    int64_t dim = 16;
    int64_t total_count = 10;
    int64_t valid_count = 5;

    std::vector<float> data(valid_count * dim);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<float>(i);
    }

    std::unique_ptr<bool[]> valid_flags = std::make_unique<bool[]>(total_count);
    for (int64_t i = 0; i < total_count; ++i) {
        if (i % 2 == 0) {
            valid_flags[i] = true;
        } else {
            valid_flags[i] = false;
        }
    }

    auto data_array = CreateVectorDataArrayFrom(
        data.data(), valid_flags.get(), total_count, valid_count, field_meta);

    std::map<FieldId, std::unique_ptr<milvus::DataArray>> output_fields_data;
    output_fields_data[vec] = std::move(data_array);

    std::vector<MergeBase> merge_bases;
    merge_bases.emplace_back(&output_fields_data, 0);
    merge_bases.back().setValidDataOffset(vec, 0);
    merge_bases.emplace_back(&output_fields_data, 2);
    merge_bases.back().setValidDataOffset(vec, 1);
    merge_bases.emplace_back(&output_fields_data, 4);
    merge_bases.back().setValidDataOffset(vec, 2);
    merge_bases.emplace_back(&output_fields_data, 6);
    merge_bases.back().setValidDataOffset(vec, 3);
    merge_bases.emplace_back(&output_fields_data, 8);
    merge_bases.back().setValidDataOffset(vec, 4);

    auto merged_result = MergeDataArray(merge_bases, field_meta);

    ASSERT_TRUE(merged_result->valid_data().size() > 0);
    ASSERT_EQ(merged_result->valid_data().size(), 5);
    ASSERT_EQ(merged_result->vectors().float_vector().data_size(), 5 * dim);

    ASSERT_TRUE(merged_result->valid_data(0));
    ASSERT_TRUE(merged_result->valid_data(1));
    ASSERT_TRUE(merged_result->valid_data(2));
    ASSERT_TRUE(merged_result->valid_data(3));
    ASSERT_TRUE(merged_result->valid_data(4));
}

// Tests for CheckCancellation utility function
TEST(UtilSegcore, CheckCancellationNullContext) {
    using namespace milvus::segcore;

    // Should not throw when op_ctx is nullptr
    EXPECT_NO_THROW(CheckCancellation(nullptr, 123, "TestOperation"));
    EXPECT_NO_THROW(CheckCancellation(nullptr, 123, 456, "TestOperation"));
}

TEST(UtilSegcore, CheckCancellationNotCancelled) {
    using namespace milvus;
    using namespace milvus::segcore;

    // Create a cancellation source that is NOT cancelled
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());

    // Should not throw when cancellation is not requested
    EXPECT_NO_THROW(CheckCancellation(&op_ctx, 123, "TestOperation"));
    EXPECT_NO_THROW(CheckCancellation(&op_ctx, 123, 456, "TestOperation"));
}

TEST(UtilSegcore, CheckCancellationCancelled) {
    using namespace milvus;
    using namespace milvus::segcore;

    // Create a cancellation source and request cancellation
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    source.requestCancellation();

    // Should throw SegcoreError with FollyCancel when cancelled
    try {
        CheckCancellation(&op_ctx, 123, "TestOperation");
        FAIL() << "Expected SegcoreError to be thrown";
    } catch (const SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
        EXPECT_TRUE(std::string(e.what()).find("TestOperation") !=
                    std::string::npos);
        EXPECT_TRUE(std::string(e.what()).find("123") != std::string::npos);
    }
}

TEST(UtilSegcore, CheckCancellationCancelledWithFieldId) {
    using namespace milvus;
    using namespace milvus::segcore;

    // Create a cancellation source and request cancellation
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    source.requestCancellation();

    // Should throw SegcoreError with field info in message
    try {
        CheckCancellation(&op_ctx, 123, 456, "LoadFieldData");
        FAIL() << "Expected SegcoreError to be thrown";
    } catch (const SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), ErrorCode::FollyCancel);
        EXPECT_TRUE(std::string(e.what()).find("LoadFieldData") !=
                    std::string::npos);
        EXPECT_TRUE(std::string(e.what()).find("123") != std::string::npos);
        EXPECT_TRUE(std::string(e.what()).find("456") != std::string::npos);
    }
}

TEST(UtilSegcore, CheckCancellationCancelAfterCheck) {
    using namespace milvus;
    using namespace milvus::segcore;

    // Create a cancellation source
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());

    // First check should pass
    EXPECT_NO_THROW(CheckCancellation(&op_ctx, 123, "TestOperation"));

    // Request cancellation
    source.requestCancellation();

    // Second check should throw
    EXPECT_THROW(CheckCancellation(&op_ctx, 123, "TestOperation"),
                 SegcoreError);
}
