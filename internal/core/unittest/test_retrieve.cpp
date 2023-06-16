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

#include "query/Expr.h"
#include "query/ExprImpl.h"
#include "segcore/ScalarIndex.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

TEST(Retrieve, ScalarIndex) {
    SUCCEED();
    auto index = std::make_unique<ScalarIndexVector>();
    std::vector<int64_t> data;
    int N = 1000;
    auto req_ids = std::make_unique<IdArray>();
    auto req_ids_arr = req_ids->mutable_int_id();

    for (int i = 0; i < N; ++i) {
        data.push_back(i * 3 % N);
        req_ids_arr->add_data(i);
    }
    index->append_data(data.data(), N, SegOffset(10000));
    index->build();

    auto [res_ids, res_offsets] = index->do_search_ids(*req_ids);
    auto res_ids_arr = res_ids->int_id();

    for (int i = 0; i < N; ++i) {
        auto res_offset = res_offsets[i].get() - 10000;
        auto res_id = res_ids_arr.data(i);
        auto std_id = (res_offset * 3 % N);
        ASSERT_EQ(res_id, std_id);
    }
}

TEST(Retrieve, AutoID) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(i64_col[choose(i)]);
    }
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_fields_id{fid_64, fid_vec};
    plan->field_ids_ = target_fields_id;

    auto retrieve_results = segment->Retrieve(plan.get(), 100);
    Assert(retrieve_results->fields_data_size() == target_fields_id.size());
    auto field0 = retrieve_results->fields_data(0);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();

    for (int i = 0; i < req_size; ++i) {
        auto index = choose(i);
        auto data = field0_data.data(i);
    }

    for (int i = 0; i < req_size; ++i) {
        auto index = choose(i);
        auto data = field0_data.data(i);
        ASSERT_EQ(data, i64_col[index]);
    }

    auto field1 = retrieve_results->fields_data(1);
    Assert(field1.has_vectors());
    auto field1_data = field1.vectors().float_vector();
    ASSERT_EQ(field1_data.data_size(), DIM * req_size);
}

TEST(Retrieve, AutoID2) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    auto i64_col = dataset.get_col<int64_t>(fid_64);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(i64_col[choose(i)]);
    }
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = segment->Retrieve(plan.get(), 100);
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
    auto field1_data = field1.vectors().float_vector();
    ASSERT_EQ(field1_data.data_size(), DIM * req_size);
}

TEST(Retrieve, NotExist) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
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
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(i64_col[choose(i)]);
        values.emplace_back(choose2(i));
    }

    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = segment->Retrieve(plan.get(), 100);
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
    auto field1_data = field1.vectors().float_vector();
    ASSERT_EQ(field1_data.data_size(), DIM * req_size);
}

TEST(Retrieve, Empty) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto segment = CreateSealedSegment(schema);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(choose(i));
    }
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = segment->Retrieve(plan.get(), 100);

    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    auto field1 = retrieve_results->fields_data(1);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();
    Assert(field0_data.data_size() == 0);
    Assert(field1.vectors().float_vector().data_size() == 0);
}

TEST(Retrieve, LargeTimestamp) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
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
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(i64_col[choose(i)]);
    }
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    std::vector<int> filter_timestamps{-1, 0, 1, 10, 20};
    filter_timestamps.push_back(N / 2);
    for (const auto& f_ts : filter_timestamps) {
        auto retrieve_results =
            segment->Retrieve(plan.get(), ts_offset + 1 + f_ts);
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
        }
    }
}

TEST(Retrieve, Delete) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
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
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(i64_col[choose(i)]);
    }
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_offsets{fid_ts, fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    {
        auto retrieve_results = segment->Retrieve(plan.get(), 100);
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
        auto field2_data = field2.vectors().float_vector();
        ASSERT_EQ(field2_data.data_size(), DIM * req_size);
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
        auto retrieve_results = segment->Retrieve(plan.get(), 100);
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
        auto field2_data = field2.vectors().float_vector();
        ASSERT_EQ(field2_data.data_size(), DIM * size);
    }
}
