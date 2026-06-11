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
#include <array>
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
#include "query/Utils.h"
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

TEST(Util_Segcore, CreateEmptyVectorDataArrayForNullableVectors) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    auto sparse_vec = schema->AddDebugField("sparse",
                                            DataType::VECTOR_SPARSE_U32_F32,
                                            0,
                                            knowhere::metric::IP,
                                            true);
    std::array<bool, 3> valid_data = {false, false, false};

    auto dense_result =
        CreateEmptyVectorDataArray(3, 0, valid_data.data(), (*schema)[vec]);
    ASSERT_EQ(dense_result->valid_data().size(), 3);
    ASSERT_FALSE(dense_result->valid_data(0));
    ASSERT_FALSE(dense_result->valid_data(1));
    ASSERT_FALSE(dense_result->valid_data(2));
    ASSERT_EQ(dense_result->vectors().float_vector().data_size(), 0);

    auto sparse_result = CreateEmptyVectorDataArray(
        3, 0, valid_data.data(), (*schema)[sparse_vec]);
    ASSERT_EQ(sparse_result->valid_data().size(), 3);
    ASSERT_FALSE(sparse_result->valid_data(0));
    ASSERT_FALSE(sparse_result->valid_data(1));
    ASSERT_FALSE(sparse_result->valid_data(2));
    ASSERT_EQ(sparse_result->vectors().data_case(),
              proto::schema::VectorField::kSparseFloatVector);
    ASSERT_EQ(sparse_result->vectors().sparse_float_vector().contents_size(),
              0);
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

TEST(Util_Segcore, MergeDataArrayWithNullableByteVectorsAppendsRows) {
    using namespace milvus;
    using namespace milvus::segcore;

    struct TestCase {
        DataType data_type;
        int64_t dim;
        std::string metric_type;
        int64_t bytes_per_row;
    };

    std::vector<TestCase> test_cases = {
        {DataType::VECTOR_BINARY, 16, knowhere::metric::HAMMING, 2},
        {DataType::VECTOR_FLOAT16, 2, knowhere::metric::L2, 4},
        {DataType::VECTOR_BFLOAT16, 2, knowhere::metric::L2, 4},
        {DataType::VECTOR_INT8, 3, knowhere::metric::L2, 3},
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(fmt::format("data_type={}", test_case.data_type));

        auto schema = std::make_shared<Schema>();
        auto vec = schema->AddDebugField("embeddings",
                                         test_case.data_type,
                                         test_case.dim,
                                         test_case.metric_type,
                                         true);
        auto& field_meta = (*schema)[vec];

        constexpr int64_t total_count = 4;
        constexpr int64_t valid_count = 3;
        std::array<bool, total_count> valid_flags = {true, false, true, true};

        std::string compact_data(valid_count * test_case.bytes_per_row, '\0');
        for (size_t i = 0; i < compact_data.size(); ++i) {
            compact_data[i] = static_cast<char>(i + 1);
        }

        auto data_array = CreateVectorDataArrayFrom(compact_data.data(),
                                                    valid_flags.data(),
                                                    total_count,
                                                    valid_count,
                                                    field_meta);

        std::map<FieldId, std::unique_ptr<milvus::DataArray>>
            output_fields_data;
        output_fields_data[vec] = std::move(data_array);

        std::vector<MergeBase> merge_bases;
        std::array<size_t, total_count> physical_offsets = {0, 0, 1, 2};
        for (size_t i = 0; i < total_count; ++i) {
            merge_bases.emplace_back(&output_fields_data, i);
            if (valid_flags[i]) {
                merge_bases.back().setValidDataOffset(vec, physical_offsets[i]);
            }
        }

        auto merged_result = MergeDataArray(merge_bases, field_meta);

        ASSERT_EQ(merged_result->valid_data().size(), total_count);
        EXPECT_TRUE(merged_result->valid_data(0));
        EXPECT_FALSE(merged_result->valid_data(1));
        EXPECT_TRUE(merged_result->valid_data(2));
        EXPECT_TRUE(merged_result->valid_data(3));

        std::string actual;
        switch (test_case.data_type) {
            case DataType::VECTOR_BINARY:
                actual = merged_result->vectors().binary_vector();
                break;
            case DataType::VECTOR_FLOAT16:
                actual = merged_result->vectors().float16_vector();
                break;
            case DataType::VECTOR_BFLOAT16:
                actual = merged_result->vectors().bfloat16_vector();
                break;
            case DataType::VECTOR_INT8:
                actual = merged_result->vectors().int8_vector();
                break;
            default:
                ThrowInfo(DataTypeInvalid, "unexpected test vector type");
        }

        ASSERT_EQ(actual.size(), compact_data.size());
        EXPECT_EQ(actual, compact_data);
    }
}

TEST(Util_Segcore, BitsetViewAllNone) {
    using namespace milvus;

    // empty view: vacuously true for both
    BitsetView empty_view;
    EXPECT_TRUE(empty_view.all());
    EXPECT_TRUE(empty_view.none());

    // sweep sizes across byte tails and the 64-byte block boundary
    for (size_t n : {1,
                     7,
                     8,
                     9,
                     63,
                     64,
                     65,
                     255,
                     256,
                     257,
                     511,
                     512,
                     513,
                     520,
                     1000,
                     4096,
                     4099}) {
        const size_t n_bytes = (n + 7) / 8;
        // bits beyond `n` are trailing garbage and must be ignored
        std::vector<uint8_t> zeros(n_bytes, 0x00);
        std::vector<uint8_t> ones(n_bytes, 0xFF);
        if ((n & 7) != 0) {
            zeros.back() = static_cast<uint8_t>(0xFF << (n & 7));
            ones.back() = static_cast<uint8_t>((1U << (n & 7)) - 1U);
        }

        BitsetView zero_view(zeros.data(), n);
        EXPECT_TRUE(zero_view.none()) << "n=" << n;
        EXPECT_FALSE(zero_view.all()) << "n=" << n;

        BitsetView one_view(ones.data(), n);
        EXPECT_TRUE(one_view.all()) << "n=" << n;
        EXPECT_FALSE(one_view.none()) << "n=" << n;

        // a single set bit at the first / middle / last position
        for (size_t pos : {size_t(0), n / 2, n - 1}) {
            auto flipped = zeros;
            if ((n & 7) != 0) {
                flipped.back() = 0x00;
            }
            flipped[pos / 8] = static_cast<uint8_t>(1U << (pos & 7));
            BitsetView v(flipped.data(), n);
            EXPECT_FALSE(v.none()) << "n=" << n << " pos=" << pos;
            if (n > 1) {
                EXPECT_FALSE(v.all()) << "n=" << n << " pos=" << pos;
            } else {
                EXPECT_TRUE(v.all()) << "n=" << n << " pos=" << pos;
            }
        }
    }
}

TEST(Util_Segcore, TransformBitsetAllFilteredAndNoFilterFastPaths) {
    using namespace milvus;

    constexpr size_t kRows = 130;  // not a multiple of 8 or 64
    std::vector<bool> valid_flags(kRows, true);
    std::unique_ptr<bool[]> valid_data(new bool[kRows]);
    std::copy(valid_flags.begin(), valid_flags.end(), valid_data.get());

    SealedOffsetMapping mapping;
    mapping.Build(valid_data.get(), kRows);

    const size_t n_bytes = (kRows + 7) / 8;
    std::vector<uint8_t> ones(n_bytes, 0xFF);
    ones.back() = static_cast<uint8_t>((1U << (kRows & 7)) - 1U);
    TargetBitmap physical_bitset;
    auto status = mapping.TransformBitset(BitsetView(ones.data(), kRows),
                                          physical_bitset);
    EXPECT_EQ(status, OffsetMapping::BitsetTransformStatus::AllFiltered);

    std::vector<uint8_t> zeros(n_bytes, 0x00);
    status = mapping.TransformBitset(BitsetView(zeros.data(), kRows),
                                     physical_bitset);
    EXPECT_EQ(status, OffsetMapping::BitsetTransformStatus::NoFilter);
}

TEST(Util_Segcore, TransformBitsetMasksNullableVectorRowsOutsideLogicalView) {
    using namespace milvus;

    std::array<bool, 3> valid_data = {true, true, true};
    GrowingOffsetMapping mapping;
    mapping.Append(valid_data.data(), valid_data.size(), 0, 0);

    // Growing search passes a logical bitset sized by the query timestamp's
    // active row count. Rows beyond that logical view are not visible yet and
    // must be masked in the transformed physical bitset.
    BitsetType logical_bitset(2);
    BitsetView logical_view(logical_bitset);

    TargetBitmap physical_bitset;
    auto status = mapping.TransformBitset(logical_view, physical_bitset);

    EXPECT_EQ(status, OffsetMapping::BitsetTransformStatus::Transformed);
    ASSERT_EQ(physical_bitset.size(), 3);
    EXPECT_FALSE(physical_bitset[0]);
    EXPECT_FALSE(physical_bitset[1]);
    EXPECT_TRUE(physical_bitset[2]);
}

TEST(Util_Segcore, TransformBitsetKeepsEmptyViewAsAllVisibleFastPath) {
    using namespace milvus;

    std::array<bool, 3> valid_data = {true, true, true};
    SealedOffsetMapping mapping;
    mapping.Build(valid_data.data(), valid_data.size());

    BitsetView all_visible;
    TargetBitmap physical_bitset;
    auto status = mapping.TransformBitset(all_visible, physical_bitset);

    EXPECT_EQ(status, OffsetMapping::BitsetTransformStatus::NoFilter);
    EXPECT_TRUE(physical_bitset.empty());
}

TEST(Util_Segcore, MergeDataArrayWithNullableVectorArrayUsesLogicalOffsets) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    constexpr int64_t dim = 4;
    auto vec = schema->AddDebugVectorArrayField("embeddings",
                                                DataType::VECTOR_FLOAT,
                                                dim,
                                                knowhere::metric::MAX_SIM,
                                                true);
    auto& field_meta = (*schema)[vec];

    bool valid_flags[] = {false, true, true};
    auto data_array = CreateEmptyVectorDataArray(3, 2, valid_flags, field_meta);
    auto* rows =
        data_array->mutable_vectors()->mutable_vector_array()->mutable_data();

    auto make_row = [dim](std::initializer_list<float> values) {
        VectorFieldProto row;
        row.set_dim(dim);
        row.mutable_float_vector()->mutable_data()->Add(values.begin(),
                                                        values.end());
        return row;
    };
    rows->Mutable(1)->CopyFrom(make_row({1.0F, 2.0F, 3.0F, 4.0F}));
    rows->Mutable(2)->CopyFrom(
        make_row({5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F}));

    std::map<FieldId, std::unique_ptr<milvus::DataArray>> output_fields_data;
    output_fields_data[vec] = std::move(data_array);

    std::vector<MergeBase> merge_bases;
    merge_bases.emplace_back(&output_fields_data, 0);
    merge_bases.emplace_back(&output_fields_data, 1);
    merge_bases.back().setValidDataOffset(vec, 0);
    merge_bases.emplace_back(&output_fields_data, 2);
    merge_bases.back().setValidDataOffset(vec, 1);

    auto merged_result = MergeDataArray(merge_bases, field_meta);

    ASSERT_EQ(merged_result->valid_data_size(), 3);
    EXPECT_FALSE(merged_result->valid_data(0));
    EXPECT_TRUE(merged_result->valid_data(1));
    EXPECT_TRUE(merged_result->valid_data(2));

    const auto& result_rows = merged_result->vectors().vector_array().data();
    ASSERT_EQ(result_rows.size(), 3);
    EXPECT_TRUE(result_rows.Get(0).has_float_vector());
    EXPECT_EQ(result_rows.Get(0).float_vector().data_size(), 0);

    ASSERT_EQ(result_rows.Get(1).float_vector().data_size(), dim);
    for (int64_t i = 0; i < dim; ++i) {
        EXPECT_FLOAT_EQ(result_rows.Get(1).float_vector().data(i),
                        static_cast<float>(i + 1));
    }

    ASSERT_EQ(result_rows.Get(2).float_vector().data_size(), dim * 2);
    for (int64_t i = 0; i < dim * 2; ++i) {
        EXPECT_FLOAT_EQ(result_rows.Get(2).float_vector().data(i),
                        static_cast<float>(i + 5));
    }
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
