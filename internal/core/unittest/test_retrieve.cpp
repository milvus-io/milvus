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

#include <gtest/gtest.h>

#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "plan/PlanNode.h"

using namespace milvus;
using namespace milvus::segcore;

std::unique_ptr<proto::segcore::RetrieveResults>
RetrieveUsingDefaultOutputSize(SegmentInterface* segment,
                               const query::RetrievePlan* plan,
                               Timestamp timestamp) {
    return segment->Retrieve(
        nullptr, plan, timestamp, DEFAULT_MAX_OUTPUT_SIZE, false);
}

using Param = DataType;
class RetrieveTest : public ::testing::TestWithParam<Param> {
 public:
    void
    SetUp() override {
        data_type = GetParam();
        is_sparse = IsSparseFloatVectorDataType(data_type);
        metric_type = is_sparse ? knowhere::metric::IP : knowhere::metric::L2;
    }

    DataType data_type;
    knowhere::MetricType metric_type;
    bool is_sparse = false;
};

INSTANTIATE_TEST_SUITE_P(RetrieveTest,
                         RetrieveTest,
                         ::testing::Values(DataType::VECTOR_FLOAT,
                                           DataType::VECTOR_SPARSE_FLOAT));

TEST_P(RetrieveTest, AutoID) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<proto::plan::GenericValue> values;
    for (int i = 0; i < req_size; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i64_col[choose(i)]);
        values.push_back(val);
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_fields_id{fid_64, fid_vec};
    plan->field_ids_ = target_fields_id;

    auto retrieve_results =
        RetrieveUsingDefaultOutputSize(segment.get(), plan.get(), 100);
    Assert(retrieve_results->fields_data_size() == target_fields_id.size());
    auto field0 = retrieve_results->fields_data(0);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();

    for (int i = 0; i < req_size; ++i) {
        auto index = choose(i);
        auto data = field0_data.data(i);
        ASSERT_EQ(data, i64_col[index]);
    }

    auto field1 = retrieve_results->fields_data(1);
    Assert(field1.has_vectors());
    if (!is_sparse) {
        auto field1_data = field1.vectors().float_vector();
        ASSERT_EQ(field1_data.data_size(), DIM * req_size);
    } else {
        auto field1_data = field1.vectors().sparse_float_vector();
        ASSERT_EQ(field1_data.contents_size(), req_size);
    }
}

TEST_P(RetrieveTest, AutoID2) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<proto::plan::GenericValue> values;
    {
        for (int i = 0; i < req_size; ++i) {
            proto::plan::GenericValue val;
            val.set_int64_val(i64_col[choose(i)]);
            values.push_back(val);
        }
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results =
        RetrieveUsingDefaultOutputSize(segment.get(), plan.get(), 100);
    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();

    for (int i = 0; i < req_size; ++i) {
        auto index = choose(i);
        auto data = field0_data.data(i);
        ASSERT_EQ(data, i64_col[index]);
    }

    auto field1 = retrieve_results->fields_data(1);
    Assert(field1.has_vectors());
    if (!is_sparse) {
        auto field1_data = field1.vectors().float_vector();
        ASSERT_EQ(field1_data.data_size(), DIM * req_size);
    } else {
        auto field1_data = field1.vectors().sparse_float_vector();
        ASSERT_EQ(field1_data.contents_size(), req_size);
    }
}

TEST_P(RetrieveTest, NotExist) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };
    auto choose2 = [=](int i) { return i * 3 % N + 3 * N; };

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<proto::plan::GenericValue> values;
    {
        for (int i = 0; i < req_size; ++i) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(i64_col[choose(i)]);
            values.push_back(val1);
            proto::plan::GenericValue val2;
            val2.set_int64_val(choose2(i));
            values.push_back(val2);
        }
    }

    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results =
        RetrieveUsingDefaultOutputSize(segment.get(), plan.get(), 100);
    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();

    for (int i = 0; i < req_size; ++i) {
        auto index = choose(i);
        auto data = field0_data.data(i);
        ASSERT_EQ(data, i64_col[index]);
    }

    auto field1 = retrieve_results->fields_data(1);
    Assert(field1.has_vectors());
    if (!is_sparse) {
        auto field1_data = field1.vectors().float_vector();
        ASSERT_EQ(field1_data.data_size(), DIM * req_size);
    } else {
        auto field1_data = field1.vectors().sparse_float_vector();
        ASSERT_EQ(field1_data.contents_size(), req_size);
    }
}

TEST_P(RetrieveTest, Empty) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto segment = CreateSealedSegment(schema);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<proto::plan::GenericValue> values;
    {
        for (int i = 0; i < req_size; ++i) {
            proto::plan::GenericValue val;
            val.set_int64_val(choose(i));
            values.push_back(val);
        }
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results =
        RetrieveUsingDefaultOutputSize(segment.get(), plan.get(), 100);

    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    auto field1 = retrieve_results->fields_data(1);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();
    Assert(field0_data.data_size() == 0);
    if (!is_sparse) {
        ASSERT_EQ(field1.vectors().float_vector().data_size(), 0);
    } else {
        ASSERT_EQ(field1.vectors().sparse_float_vector().contents_size(), 0);
    }
}

TEST_P(RetrieveTest, Limit) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    int64_t N = 101;
    auto dataset = DataGen(schema, N, 42);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    proto::plan::GenericValue unary_val;
    unary_val.set_int64_val(0);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        OpType::GreaterEqual,
        unary_val, std::vector<proto::plan::GenericValue>{});
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = milvus::test::CreateRetrievePlanByExpr(expr);

    // test query results exceed the limit size
    std::vector<FieldId> target_fields{TimestampFieldID, fid_64, fid_vec};
    plan->field_ids_ = target_fields;
    EXPECT_THROW(segment->Retrieve(nullptr, plan.get(), N, 1, false),
                 std::runtime_error);

    auto retrieve_results = segment->Retrieve(
        nullptr, plan.get(), N, DEFAULT_MAX_OUTPUT_SIZE, false);
    Assert(retrieve_results->fields_data_size() == target_fields.size());
    auto field0 = retrieve_results->fields_data(0);
    auto field2 = retrieve_results->fields_data(2);
    Assert(field0.scalars().long_data().data_size() == N);
    if (!is_sparse) {
        Assert(field2.vectors().float_vector().data_size() == N * DIM);
    } else {
        Assert(field2.vectors().sparse_float_vector().contents_size() == N);
    }
}

TEST_P(RetrieveTest, FillEntry) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_bool = schema->AddDebugField("bool", DataType::BOOL);
    auto fid_f32 = schema->AddDebugField("f32", DataType::FLOAT);
    auto fid_f64 = schema->AddDebugField("f64", DataType::DOUBLE);
    auto fid_vec =
        schema->AddDebugField("vector", data_type, DIM, knowhere::metric::L2);
    auto fid_vecbin = schema->AddDebugField(
        "vec_bin", DataType::VECTOR_BINARY, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 101;
    auto dataset = DataGen(schema, N, 42);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    proto::plan::GenericValue unary_val;
    unary_val.set_int64_val(0);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        OpType::GreaterEqual,
        unary_val, std::vector<proto::plan::GenericValue>{});
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = milvus::test::CreateRetrievePlanByExpr(expr);
    // test query results exceed the limit size
    std::vector<FieldId> target_fields{TimestampFieldID,
                                       fid_64,
                                       fid_bool,
                                       fid_f32,
                                       fid_f64,
                                       fid_vec,
                                       fid_vecbin};
    plan->field_ids_ = target_fields;
    EXPECT_THROW(segment->Retrieve(nullptr, plan.get(), N, 1, false),
                 std::runtime_error);

    auto retrieve_results = segment->Retrieve(
        nullptr, plan.get(), N, DEFAULT_MAX_OUTPUT_SIZE, false);
    Assert(retrieve_results->fields_data_size() == target_fields.size());
}

TEST_P(RetrieveTest, LargeTimestamp) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    int choose_sep = 3;
    auto choose = [=](int i) { return i * choose_sep % N; };
    uint64_t ts_offset = 100;
    auto dataset = DataGen(schema, N, 42, ts_offset + 1);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<proto::plan::GenericValue> values;
    {
        for (int i = 0; i < req_size; ++i) {
            proto::plan::GenericValue val;
            val.set_int64_val(i64_col[choose(i)]);
            values.push_back(val);
        }
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    ;
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    std::vector<int> filter_timestamps{-1, 0, 1, 10, 20};
    filter_timestamps.push_back(N / 2);
    for (const auto& f_ts : filter_timestamps) {
        auto retrieve_results = RetrieveUsingDefaultOutputSize(
            segment.get(), plan.get(), ts_offset + 1 + f_ts);
        Assert(retrieve_results->fields_data_size() == 2);

        int target_num = (f_ts + choose_sep) / choose_sep;
        if (target_num > req_size) {
            target_num = req_size;
        }

        for (auto field_data : retrieve_results->fields_data()) {
            if (DataType(field_data.type()) == DataType::INT64) {
                Assert(field_data.scalars().long_data().data_size() ==
                       target_num);
            }
            if (DataType(field_data.type()) == DataType::VECTOR_FLOAT) {
                Assert(field_data.vectors().float_vector().data_size() ==
                       target_num * DIM);
            }
            if (DataType(field_data.type()) == DataType::VECTOR_SPARSE_FLOAT) {
                Assert(field_data.vectors()
                           .sparse_float_vector()
                           .contents_size() == target_num);
            }
        }
    }
}

TEST_P(RetrieveTest, Delete) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec =
        schema->AddDebugField("vector_64", data_type, DIM, metric_type);
    schema->set_primary_field_id(fid_64);

    auto fid_ts = schema->AddDebugField("Timestamp", DataType::INT64);

    int64_t N = 10;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i; };

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);
    auto ts_col = dataset.get_col<int64_t>(fid_ts);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<int64_t> timestamps;
    for (int i = 0; i < req_size; ++i) {
        timestamps.emplace_back(ts_col[choose(i)]);
    }
    std::vector<proto::plan::GenericValue> values;
    {
        for (int i = 0; i < req_size; ++i) {
            proto::plan::GenericValue val;
            val.set_int64_val(i64_col[choose(i)]);
            values.push_back(val);
        }
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_offsets{fid_ts, fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    {
        auto retrieve_results =
            RetrieveUsingDefaultOutputSize(segment.get(), plan.get(), 100);
        ASSERT_EQ(retrieve_results->fields_data_size(), target_offsets.size());
        auto field0 = retrieve_results->fields_data(0);
        Assert(field0.has_scalars());
        auto field0_data = field0.scalars().long_data();

        for (int i = 0; i < req_size; ++i) {
            auto index = choose(i);
            auto data = field0_data.data(i);
            ASSERT_EQ(data, ts_col[index]);
        }

        auto field1 = retrieve_results->fields_data(1);
        Assert(field1.has_scalars());
        auto field1_data = field1.scalars().long_data();

        for (int i = 0; i < req_size; ++i) {
            auto index = choose(i);
            auto data = field1_data.data(i);
            ASSERT_EQ(data, i64_col[index]);
        }

        auto field2 = retrieve_results->fields_data(2);
        Assert(field2.has_vectors());
        if (!is_sparse) {
            auto field2_data = field2.vectors().float_vector();
            ASSERT_EQ(field2_data.data_size(), DIM * req_size);
        } else {
            auto field2_data = field2.vectors().sparse_float_vector();
            ASSERT_EQ(field2_data.contents_size(), req_size);
        }
    }

    int64_t row_count = 0;
    // strange, when enable load_delete_record, this test failed
    auto load_delete_record = false;
    if (load_delete_record) {
        std::vector<idx_t> pks{1, 2, 3, 4, 5};
        auto ids = std::make_unique<IdArray>();
        ids->mutable_int_id()->mutable_data()->Add(pks.begin(), pks.end());

        std::vector<Timestamp> timestamps{10, 10, 10, 10, 10};

        LoadDeletedRecordInfo info = {timestamps.data(), ids.get(), row_count};
        segment->LoadDeletedRecord(info);
        row_count = 5;
    }

    int64_t new_count = 6;
    std::vector<idx_t> new_pks{0, 1, 2, 3, 4, 5};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(new_pks.begin(), new_pks.end());
    std::vector<idx_t> new_timestamps{10, 10, 10, 10, 10, 10};
    auto reserved_offset = segment->get_deleted_count();
    ASSERT_EQ(reserved_offset, row_count);
    segment->Delete(reserved_offset,
                    new_count,
                    ids.get(),
                    reinterpret_cast<const Timestamp*>(new_timestamps.data()));

    {
        auto retrieve_results =
            RetrieveUsingDefaultOutputSize(segment.get(), plan.get(), 100);
        Assert(retrieve_results->fields_data_size() == target_offsets.size());
        auto field1 = retrieve_results->fields_data(1);
        Assert(field1.has_scalars());
        auto field1_data = field1.scalars().long_data();
        auto size = req_size - new_count;
        for (int i = 0; i < size; ++i) {
            auto index = choose(i);
            auto data = field1_data.data(i);
            ASSERT_EQ(data, i64_col[index + new_count]);
        }

        auto field2 = retrieve_results->fields_data(2);
        Assert(field2.has_vectors());
        if (!is_sparse) {
            auto field2_data = field2.vectors().float_vector();
            ASSERT_EQ(field2_data.data_size(), DIM * size);
        } else {
            auto field2_data = field2.vectors().sparse_float_vector();
            ASSERT_EQ(field2_data.contents_size(), size);
        }
    }
}
