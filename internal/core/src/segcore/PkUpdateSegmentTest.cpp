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

#include "gtest/gtest.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <future>
#include "query/PlanProto.h"
#include "common/Utils.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "segcore/retrieve_result_export_c.h"
#include "segcore/search_result_export_c.h"
#include "pb/cgo_msg.pb.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/UpdatesRecord.h"
#include "segcore/segment_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::segcore {
namespace {

UpdateAssignment
Long(FieldId field, int64_t value) {
    UpdateAssignment assignment{field, {}};
    assignment.literal.mutable_scalar_value()->set_long_data(value);
    return assignment;
}
IdArray
Keys(std::initializer_list<int64_t> keys) {
    IdArray ids;
    for (auto key : keys) ids.mutable_int_id()->add_data(key);
    return ids;
}

TEST(PkUpdateRecord, ReplayIdentityAtomicityAndHistory) {
    auto schema = std::make_shared<Schema>();
    auto age = schema->AddDebugField("age", DataType::INT64, true);
    auto name = schema->AddDebugField("name", DataType::VARCHAR);
    UpdateAssignment text{name, {}};
    text.literal.mutable_scalar_value()->set_string_data("old");
    UpdateAssignment null{age, {}};
    null.literal.set_null_value(google::protobuf::NULL_VALUE);
    UpdatesRecord store;
    auto apply = [&](std::vector<int64_t> rows,
                     Timestamp ts,
                     uint32_t ordinal,
                     std::vector<UpdateAssignment> assignments) {
        store.Apply(*schema, 10000, rows, ts, ordinal, assignments);
    };
    apply({10, 10}, 100, 0, {Long(age, 11), text});
    auto owned = std::get<UpdateString>(*store.Lookup(name, 10, 100));
    apply({20}, 200, 1, {Long(age, 22)});
    apply({10}, 200, 4, {Long(age, 11)});
    apply({10}, 200, 3, {null});
    apply({10}, 140, 0, {null});
    apply({10, 30}, 100, 0, {Long(age, 11), text});
    apply({30, 10}, 100, 0, {Long(age, 11), text});
    EXPECT_EQ(store.Stats(age).entries, 6);
    EXPECT_EQ(store.Stats(age).distinct_offsets, 3);
    EXPECT_EQ(store.Stats(age).membership_size, 10000);
    EXPECT_FALSE(store.Lookup(age, 10, 99));
    EXPECT_FALSE(store.Lookup(age, 20, 140));
    EXPECT_FALSE(store.Lookup(age, 99, 300));
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(age, 10, 139)), 11);
    EXPECT_TRUE(
        std::holds_alternative<std::monostate>(*store.Lookup(age, 10, 199)));
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(age, 10, 200)), 11);

    text.literal.mutable_scalar_value()->set_string_data("new");
    EXPECT_THROW(apply({10, 40}, 100, 0, {Long(age, 11), text}), SegcoreError);
    EXPECT_FALSE(store.Lookup(age, 40, 300));
    EXPECT_THROW(apply({40}, 300, 0, {Long(age, 11), Long(name, 5)}),
                 SegcoreError);
    EXPECT_THROW(store.Apply(*schema,
                             10000,
                             {10, 40},
                             300,
                             0,
                             {Long(age, 33), text},
                             [] { throw std::bad_alloc(); }),
                 std::bad_alloc);
    EXPECT_EQ(store.Stats(age).entries, 6);
    EXPECT_EQ(store.Stats(name).entries, 2);
    apply({10, 40}, 300, 0, {Long(age, 33), text});
    EXPECT_EQ(*owned, "old");
    EXPECT_EQ(*std::get<UpdateString>(*store.Lookup(name, 10, 300)), "new");
}

TEST(PkUpdateRecord, GCRebuildsHistoriesAndProtectsReplayBoundary) {
    Schema schema;
    const auto score = schema.AddDebugField("score", DataType::INT64, true);
    const auto expanded = schema.AddDebugField("expanded", DataType::INT64);
    UpdatesRecord store;
    auto apply =
        [&](int row, Timestamp ts, uint32_t ordinal, UpdateAssignment a) {
            store.Apply(schema, 8, {row}, ts, ordinal, {a});
        };
    UpdateAssignment null{score, {}};
    null.literal.set_null_value(google::protobuf::NULL_VALUE);
    apply(0, 10, 0, Long(score, 10));
    apply(1, 15, 0, Long(score, 15));
    apply(0, 20, 0, Long(score, 20));
    apply(0, 20, 2, null);
    apply(2, 35, 0, Long(score, 35));
    apply(0, 40, 0, Long(score, 40));

    store.GC(20);
    // Includes the old winner for row 1 and only future versions for row 2.
    auto stats = store.Stats(score);
    EXPECT_EQ(stats.entries, 4);
    EXPECT_EQ(stats.distinct_offsets, 3);
    EXPECT_EQ(stats.membership_size, 8);
    EXPECT_FALSE(store.Lookup(score, 2, 20));
    EXPECT_FALSE(store.Lookup(score, 3, 40));
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(score, 1, 20)), 15);
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(score, 0, 40)), 40);
    std::map<int64_t, UpdateValue> visible;
    store.VisitUpdates(score, 20, [&](int64_t row, const UpdateValue& value) {
        visible[row] = value;
    });
    EXPECT_EQ(visible.size(), 2);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(visible.at(0)));
    EXPECT_EQ(std::get<int64_t>(visible.at(1)), 15);

    // An out-of-order suffix rebuild must carry the completed prefix with it.
    apply(0, 30, 0, Long(score, 30));
    apply(0, 10, 0, Long(score, 10));
    apply(0, 20, 0, Long(score, 20));
    apply(0, 20, 2, null);
    EXPECT_THROW(apply(0, 20, 2, Long(score, 99)), SegcoreError);
    EXPECT_EQ(store.Stats(score).entries, 5);
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(score, 0, 39)), 30);
    // A newly introduced field on the same physical segment can backfill.
    apply(0, 5, 0, Long(expanded, 5));
    apply(0, 15, 0, Long(expanded, 15));
    store.GC(20);
    store.GC(8);
    EXPECT_EQ(store.Stats(expanded).entries, 2);
    EXPECT_EQ(store.Stats(score).entries, 5);
    store.GC(40);
    EXPECT_EQ(store.Stats(expanded).entries, 1);
    EXPECT_EQ(store.Stats(score).entries, 3);
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(expanded, 0, 40)), 15);
}

TEST(PkUpdateRecord, OutOfOrderFragmentsMatchIndependentReplay) {
    Schema schema;
    auto field = schema.AddDebugField("value", DataType::INT64);
    UpdatesRecord store;
    struct Mutation {
        Timestamp ts;
        uint32_t ordinal;
        int64_t value;
        std::vector<int64_t> rows;
    };
    std::vector<Mutation> mutations;
    for (int i = 0; i < 31; ++i) {
        mutations.push_back({100 + static_cast<Timestamp>(i / 3),
                             static_cast<uint32_t>(i % 3),
                             i,
                             {0, 1 + i % 4, 5 + i % 8}});
    }
    // Golden log is in commit/body order. Deliver historical and live
    // fragments in a permutation, overlapping one key on each recut.
    for (size_t step = 0; step < mutations.size(); ++step) {
        const auto& m = mutations[(step * 13) % mutations.size()];
        store.Apply(schema,
                    16,
                    {m.rows[1], m.rows[2]},
                    m.ts,
                    m.ordinal,
                    {Long(field, m.value)});
        store.Apply(schema,
                    16,
                    {m.rows[0], m.rows[1]},
                    m.ts,
                    m.ordinal,
                    {Long(field, m.value)});
    }
    EXPECT_EQ(store.Stats(field).entries, mutations.size() * 3);
    for (Timestamp ts = 99; ts <= 111; ++ts) {
        std::map<int64_t, int64_t> reverse;
        store.VisitUpdates(
            field, ts, [&](int64_t row, const UpdateValue& value) {
                EXPECT_TRUE(
                    reverse.emplace(row, std::get<int64_t>(value)).second);
            });
        for (int64_t row = 0; row < 16; ++row) {
            std::optional<int64_t> expected;
            for (const auto& m : mutations) {
                if (m.ts <= ts &&
                    std::find(m.rows.begin(), m.rows.end(), row) !=
                        m.rows.end()) {
                    expected = m.value;
                }
            }
            auto actual = store.Lookup(field, row, ts);
            ASSERT_EQ(actual.has_value(), expected.has_value());
            EXPECT_EQ(reverse.count(row), expected.has_value());
            if (expected) {
                EXPECT_EQ(reverse.at(row), *expected);
                EXPECT_EQ(std::get<int64_t>(*actual), *expected);
            }
        }
    }
}

TEST(PkUpdateRecord, VisiblePrefixAndMaskedRange) {
    Schema schema;
    auto field = schema.AddDebugField("score", DataType::INT64, true);
    UpdatesRecord record;
    record.Apply(schema, 32, {10}, 100, 0, {Long(field, 4)});
    record.Apply(schema, 32, {20}, 200, 0, {Long(field, 5)});
    auto select = [&](Timestamp ts, auto expected) {
        std::map<int64_t, UpdateValue> actual;
        auto stats = record.VisitUpdates(
            field, ts, [&](int64_t row, const UpdateValue& value) {
                EXPECT_TRUE(actual.emplace(row, value).second);
            });
        EXPECT_EQ(actual, expected);
        return stats;
    };
    using Values = std::map<int64_t, UpdateValue>;
    auto stats = select(140, Values{{10, int64_t(4)}});
    EXPECT_FALSE(stats.used_visited);
    EXPECT_EQ(stats.scanned_entries, 1);
    UpdateAssignment null{field, {}};
    null.literal.set_null_value(google::protobuf::NULL_VALUE);
    record.Apply(schema, 32, {10}, 100, 1, {null});
    stats = select(200, Values{{10, {}}, {20, int64_t(5)}});
    EXPECT_TRUE(stats.used_visited);
    EXPECT_EQ(stats.scanned_entries, 2);
    int32_t rows[] = {2, 10, 20, 10, 10, 5};
    TargetBitmap mask(6, true);
    mask[1] = false;
    auto patches =
        record.ReadRangeUpdates(field, 140, 0, 6, rows, mask.view(), 0, &stats);
    ASSERT_EQ(patches.size(), 2);
    EXPECT_EQ(patches[0].position, 3);
    EXPECT_EQ(patches[1].position, 4);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(patches[0].value));
    EXPECT_EQ(stats.dictionary_probes, 3);

    const int64_t untouched[] = {2, -1, 1, 2};
    auto before = record.Diagnostics().dictionary_probes;
    EXPECT_TRUE(record.Gather(field, 140, untouched, 4).empty());
    EXPECT_EQ(record.Diagnostics().dictionary_probes, before);

    const int64_t ordered[] = {20, 10, -1, 10, 2};
    auto gathered = record.Gather(field, 140, ordered, 5);
    ASSERT_EQ(gathered.size(), 2);
    EXPECT_EQ(gathered[0].position, 1);
    EXPECT_EQ(gathered[1].position, 3);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(gathered[1].value));
    EXPECT_EQ(record.Diagnostics().dictionary_probes - before, 3);
}

TEST(PkUpdateRecord, TypedLiteralsAndWholeArrayOwnership) {
    auto schema = std::make_shared<Schema>();
    std::vector<UpdateAssignment> assignments;
    auto scalar = [&](DataType type) {
        auto id =
            schema->AddDebugField(std::to_string(assignments.size()), type);
        return assignments.emplace_back(UpdateAssignment{id, {}})
            .literal.mutable_scalar_value();
    };
    scalar(DataType::BOOL)->set_bool_data(true);
    scalar(DataType::INT8)->set_int_data(-128);
    scalar(DataType::INT16)->set_int_data(32767);
    scalar(DataType::INT32)->set_int_data(-123456);
    scalar(DataType::INT64)->set_long_data(12345678901LL);
    scalar(DataType::TIMESTAMPTZ)->set_timestamptz_data(-12345678901LL);
    scalar(DataType::VARCHAR)->set_string_data("owned");
    auto array = [&](DataType type) {
        auto id = schema->AddDebugArrayField(
            std::to_string(assignments.size()), type, false);
        return assignments.emplace_back(UpdateAssignment{id, {}})
            .literal.mutable_array_value();
    };
    array(DataType::BOOL)->mutable_bool_data()->add_data(true);
    array(DataType::INT8)->mutable_int_data()->add_data(1);
    array(DataType::INT16)->mutable_int_data()->add_data(222);
    array(DataType::INT32)->mutable_int_data()->add_data(-44444);
    array(DataType::INT64)->mutable_long_data()->add_data(22222222222LL);
    array(DataType::FLOAT)->mutable_float_data()->add_data(1.5);
    array(DataType::DOUBLE)->mutable_double_data()->add_data(2.75);
    array(DataType::VARCHAR)->mutable_string_data()->add_data("element");

    UpdatesRecord store;
    store.Apply(*schema, 2, {1}, 10, 0, assignments);
    auto read = [&](int i) {
        return store.Lookup(assignments[i].field, 1, 10).value();
    };
    EXPECT_TRUE(std::get<bool>(read(0)));
    EXPECT_EQ(std::get<int8_t>(read(1)), -128);
    EXPECT_EQ(std::get<int16_t>(read(2)), 32767);
    EXPECT_EQ(std::get<int32_t>(read(3)), -123456);
    EXPECT_EQ(std::get<int64_t>(read(4)), 12345678901LL);
    EXPECT_EQ(std::get<int64_t>(read(5)), -12345678901LL);
    EXPECT_EQ(*std::get<UpdateString>(read(6)), "owned");
    for (size_t i = 7; i < assignments.size(); ++i) {
        auto retained = std::get<UpdateArray>(read(i));
        EXPECT_EQ(retained->SerializeAsString(),
                  assignments[i].literal.array_value().SerializeAsString());
        EXPECT_NE(retained.get(), &assignments[i].literal.array_value());
    }
    auto& last = assignments.back();
    auto retained = std::get<UpdateArray>(read(assignments.size() - 1));
    last.literal.mutable_array_value()->mutable_string_data()->clear_data();
    store.Apply(*schema, 2, {1}, 10, 1, {last});
    const int64_t row = 1;
    auto old_text = store.Gather(assignments[6].field, 10, &row, 1);
    auto text = assignments[6];
    text.literal.mutable_scalar_value()->set_string_data("replacement");
    store.Apply(*schema, 2, {1}, 10, 2, {text});
    store.GC(10);
    EXPECT_EQ(store.Stats(last.field).entries, 1);
    EXPECT_EQ(store.Stats(text.field).entries, 1);
    EXPECT_EQ(*std::get<UpdateString>(old_text.at(0).value), "owned");
    EXPECT_EQ(retained->string_data().data(0), "element");
    EXPECT_EQ(std::get<UpdateArray>(read(assignments.size() - 1))
                  ->string_data()
                  .data_size(),
              0);
}

GeneratedData
MakeRows(const SchemaPtr& schema, FieldId pk, std::vector<Timestamp> times) {
    auto data = DataGen(schema, times.size(), 42);
    data.timestamps_ = std::move(times);
    for (auto& field : *data.raw_->mutable_fields_data()) {
        if (field.field_id() == pk.get()) {
            auto* values = field.mutable_scalars()->mutable_long_data();
            for (int i = 0; i < values->data_size(); ++i)
                values->set_data(i, 42);
        }
    }
    return data;
}

TEST(PkUpdateSegment, HistoricalRetrieveOutput) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto age = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, nullptr);
    auto data = MakeRows(schema, pk, {1, 2});
    segment->PreInsert(2);
    segment->Insert(
        0, 2, data.row_ids_.data(), data.timestamps_.data(), data.SharedRaw());
    segment->ApplyUpdates(Keys({42}), 20, 0, {Long(age, 371)});
    segment->ApplyUpdates(Keys({42}), 30, 0, {Long(age, 589)});
    proto::plan::PlanNode node;
    node.add_output_field_ids(age.get());
    node.mutable_query()->set_limit(2);
    auto plan = query::ProtoParser(schema).CreateRetrievePlan(node);
    for (auto [ts, value] : {std::pair{20, 371}, {29, 371}, {30, 589}}) {
        auto result =
            segment->Retrieve(nullptr, plan.get(), ts, 1 << 20, false);
        ASSERT_EQ(result->fields_data_size(), 1);
        const auto& values = result->fields_data(0).scalars().long_data();
        ASSERT_GT(values.data_size(), 0);
        for (auto actual : values.data()) EXPECT_EQ(actual, value);
    }
}

TEST(PkUpdateSegment, DeferredOutputAndGroupKeysUsePinnedVersions) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto name = schema->AddDebugField("name", DataType::VARCHAR, true);
    auto values = schema->AddDebugArrayField("values", DataType::INT64, true);
    schema->set_primary_field_id(pk);
    auto data = MakeRows(schema, pk, {1, 2, 20});
    for (auto& f : *data.raw_->mutable_fields_data()) {
        if (f.field_id() == pk.get()) {
            f.mutable_scalars()->mutable_long_data()->set_data(2, 43);
            continue;
        }
        f.clear_valid_data();
        for (bool valid : {true, true, true}) f.add_valid_data(valid);
        if (f.field_id() == name.get()) {
            auto* d = f.mutable_scalars()->mutable_string_data();
            d->clear_data();
            for (const auto* text : {"a", "b", "c"}) d->add_data(text);
        } else {
            auto* d = f.mutable_scalars()->mutable_array_data();
            d->clear_data();
            for (auto n : {1, 2, 3})
                d->add_data()->mutable_long_data()->add_data(n);
        }
    }
    auto sealed = CreateSealedSegment(schema, nullptr, 87433);
    LoadGeneratedDataIntoSegment(data, sealed.get());
    auto fork =
        dynamic_cast<ChunkedSegmentSealedImpl*>(sealed.get())->PinReadState();
    UpdateAssignment text{name, {}}, array{values, {}}, null{name, {}};
    text.literal.mutable_scalar_value()->set_string_data("visible");
    array.literal.mutable_array_value()->mutable_long_data()->add_data(123);
    null.literal.set_null_value(google::protobuf::NULL_VALUE);
    sealed->ApplyUpdates(Keys({42}), 20, 0, {text, array});
    sealed->ApplyUpdates(Keys({42}), 30, 0, {null});

    query::Plan plan(schema);
    plan.target_entries_ = {name, values};
    auto search_fields = [&](Timestamp ts) {
        SearchResult output;
        output.read_snapshot_ = fork->CaptureReadSnapshot();
        output.query_timestamp_ = ts;
        output.seg_offsets_ = {2, 0, -1, 0};
        output.distances_.resize(4);
        fork->TestFillTargetEntry(&plan, output);
        return std::move(output.output_fields_data_);
    };
    auto old = search_fields(19);
    auto current = search_fields(20);
    auto latest = search_fields(30);
    EXPECT_EQ(old.at(name)->scalars().string_data().data(1), "a");
    EXPECT_EQ(current.at(name)->scalars().string_data().data(0), "c");
    EXPECT_EQ(current.at(name)->scalars().string_data().data(3), "visible");
    EXPECT_EQ(
        current.at(values)->scalars().array_data().data(3).long_data().data(0),
        123);
    EXPECT_FALSE(GetFieldDataRowValidData(*current.at(name))[2]);
    EXPECT_FALSE(GetFieldDataRowValidData(*latest.at(name))[1]);
    EXPECT_TRUE(GetFieldDataRowValidData(*latest.at(name))[0]);
    for (auto ts : {19, 20, 30}) {
        auto snapshot = fork->CaptureReadSnapshot();
        exec::MultiFieldDataGetter getter(nullptr,
                                          *fork,
                                          {name, pk},
                                          {},
                                          {},
                                          false,
                                          snapshot.get(),
                                          true,
                                          ts);
        CompositeGroupKey key;
        getter.GetInto(0, key);
        if (ts == 30)
            EXPECT_FALSE(key[0].has_value());
        else
            EXPECT_EQ(std::get<std::string>(key[0].value()),
                      ts == 19 ? "a" : "visible");
        EXPECT_EQ(std::get<int64_t>(key[1].value()), 42);
    }
    query::RetrievePlan retrieve(schema);
    retrieve.field_ids_ = {name};
    CSegmentInterface segments[] = {fork.get()};
    int32_t indices[] = {0, 0, 0};
    int64_t offsets[] = {1, 2, 1};
    for (auto ts : {19, 20, 30}) {
        SearchResult hit;
        hit.segment_ = fork.get();
        hit.read_lease_ = fork->AcquireReadLease({});
        hit.read_snapshot_ = fork->CaptureReadSnapshot();
        hit.query_timestamp_ = ts;
        CSearchResult hits[] = {&hit};
        CProto serialized{};
        auto proto_status = FillOutputFieldsOrdered(
            hits, 1, &plan, indices, offsets, 3, &serialized, nullptr);
        ASSERT_EQ(proto_status.error_code, Success) << proto_status.error_msg;
        proto::schema::SearchResultData decoded;
        auto parsed = decoded.ParseFromArray(serialized.proto_blob,
                                             serialized.proto_size);
        free(const_cast<void*>(serialized.proto_blob));
        ASSERT_TRUE(parsed);
        ASSERT_EQ(decoded.fields_data_size(), 2);
        const auto& names = decoded.fields_data(0);
        ASSERT_EQ(names.field_id(), name.get());
        EXPECT_EQ(GetFieldDataRowValidData(names)[0], ts != 30);
        EXPECT_EQ(names.scalars().string_data().data(1), "c");
        if (ts != 30)
            EXPECT_EQ(names.scalars().string_data().data(2),
                      ts == 19 ? "b" : "visible");

        proto::cgo::FunctionChainInputPlan projection;
        auto* col_projection = projection.add_inputs();
        col_projection->set_source_field_id(name.get());
        col_projection->set_target_data_type(proto::schema::VarChar);
        col_projection->set_logical_name("name");
        const auto input = projection.SerializeAsString();
        ArrowSchema logical_schema{};
        ArrowArray logical_data{};
        auto logical_status =
            FillFieldsOrderedAsArrowRecordBatchWithInputPlan(hits,
                                                             1,
                                                             &plan,
                                                             input.data(),
                                                             input.size(),
                                                             indices,
                                                             offsets,
                                                             3,
                                                             &logical_schema,
                                                             &logical_data,
                                                             nullptr);
        ASSERT_EQ(logical_status.error_code, Success)
            << logical_status.error_msg;
        auto logical_batch =
            arrow::ImportRecordBatch(&logical_data, &logical_schema);
        ASSERT_TRUE(logical_batch.ok()) << logical_batch.status();
        auto strings = (*logical_batch)->column(0);
        EXPECT_EQ(strings->IsNull(0), ts == 30);
        EXPECT_EQ((*strings->GetScalar(1))->ToString(), "c");
        if (ts != 30)
            EXPECT_EQ((*strings->GetScalar(2))->ToString(),
                      ts == 19 ? "b" : "visible");
        hit.read_lease_.reset();
        ArrowSchema as{};
        ArrowArray aa{};
        auto status = FillRetrieveFieldsOrdered(segments,
                                                1,
                                                &retrieve,
                                                ts,
                                                indices,
                                                offsets,
                                                3,
                                                1 << 20,
                                                &as,
                                                &aa,
                                                nullptr);
        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        auto batch = arrow::ImportRecordBatch(&aa, &as);
        ASSERT_TRUE(batch.ok()) << batch.status();
        auto col = (*batch)->column(0);
        ASSERT_EQ(col->length(), 3);
        for (int pos : {0, 2}) {
            EXPECT_EQ(col->IsNull(pos), ts == 30);
            if (ts != 30)
                EXPECT_EQ((*col->GetScalar(pos))->ToString(),
                          ts == 19 ? "b" : "visible");
        }
        EXPECT_EQ((*col->GetScalar(1))->ToString(), "c");
    }
}

TEST(PkUpdateSegment, OrderByAndAggregationReadAtQueryTimestamp) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto score = schema->AddDebugField("score", DataType::INT64);
    auto label = schema->AddDebugField("label", DataType::VARCHAR);
    schema->set_primary_field_id(pk);
    auto data = DataGen(schema, 3);
    std::fill(data.timestamps_.begin(), data.timestamps_.end(), 1);
    for (auto& field : *data.raw_->mutable_fields_data()) {
        for (int i = 0; i < 3; ++i) {
            if (field.field_id() == label.get())
                field.mutable_scalars()->mutable_string_data()->set_data(
                    i, std::to_string(i));
            else
                field.mutable_scalars()->mutable_long_data()->set_data(i, i);
        }
    }
    auto segment = CreateSealedSegment(schema);
    LoadGeneratedDataIntoSegment(data, segment.get());
    UpdateAssignment text{label, {}};
    text.literal.mutable_scalar_value()->set_string_data("winner");
    segment->ApplyUpdates(Keys({2}), 20, 0, {Long(score, -8), text});
    proto::plan::PlanNode order_node, sum_node;
    for (auto field : {pk, score, label})
        order_node.add_output_field_ids(field.get());
    auto* order = order_node.mutable_query()->add_order_by_fields();
    order->set_field_id(score.get());
    order->set_ascending(true);
    order_node.mutable_query()->set_limit(1);
    auto* sum = sum_node.mutable_query()->add_aggregates();
    sum->set_op(proto::plan::sum);
    sum->set_field_id(score.get());
    auto order_plan = query::ProtoParser(schema).CreateRetrievePlan(order_node);
    auto sum_plan = query::ProtoParser(schema).CreateRetrievePlan(sum_node);
    for (auto ts : {19, 20}) {
        auto result =
            segment->Retrieve(nullptr, order_plan.get(), ts, 1 << 20, false);
        ASSERT_EQ(result->ids().int_id().data_size(), 1);
        EXPECT_EQ(result->ids().int_id().data(0), ts == 19 ? 0 : 2);
        bool found_label = false;
        for (const auto& field : result->fields_data()) {
            if (field.field_id() == label.get()) {
                found_label = true;
                EXPECT_EQ(field.scalars().string_data().data(0),
                          ts == 19 ? "0" : "winner");
            }
        }
        EXPECT_TRUE(found_label);
        auto sums =
            segment->Retrieve(nullptr, sum_plan.get(), ts, 1 << 20, false);
        ASSERT_EQ(sums->fields_data_size(), 1);
        const auto& totals = sums->fields_data(0).scalars().long_data();
        ASSERT_EQ(totals.data_size(), 1);
        EXPECT_EQ(totals.data(0), ts == 19 ? 3 : -7);
    }
}

TEST(PkUpdateSegment, GrowingInsertDeleteAndReplay) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto age = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, nullptr);
    auto insert = [&](std::vector<Timestamp> times) {
        auto data = MakeRows(schema, pk, times);
        const auto begin = segment->PreInsert(times.size());
        segment->Insert(begin,
                        times.size(),
                        data.row_ids_.data(),
                        times.data(),
                        data.SharedRaw());
    };
    insert({1, 2, 10});
    auto ids = Keys({42, 42});
    auto write = [&](Timestamp ts, int64_t value) {
        segment->ApplyUpdates(ids, ts, 0, {Long(age, value)});
    };
    write(10, 111);
    const auto& store = segment->GetUpdatesRecord();
    EXPECT_EQ(store.Stats(age).entries, 2);
    EXPECT_FALSE(store.Lookup(age, 2, 10));
    Timestamp deleted = 11;
    auto single = Keys({42});
    segment->Delete(1, &single, &deleted);
    insert({12, 13});
    EXPECT_EQ(store.Stats(age).membership_size, 5);
    write(14, 222);
    write(10, 111);
    EXPECT_EQ(store.Stats(age).entries, 4);
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(age, 0, 14)), 111);
    EXPECT_FALSE(store.Lookup(age, 2, 14));
    EXPECT_EQ(std::get<int64_t>(*store.Lookup(age, 4, 14)), 222);
}

TEST(PkUpdateSegment, ProjectedPKReturnsResourceStatus) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto age = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto layout =
        std::make_shared<SealedFieldLayout>(schema, LoadScope({age.get()}));
    ChunkedSegmentSealedImpl segment(
        layout, nullptr, SegcoreConfig::default_config(), 87192);
    auto keys = Keys({42}).SerializeAsString();
    auto value = Long(age, 9).literal.SerializeAsString();
    CProto literal{value.data(), static_cast<int64_t>(value.size())};
    int64_t field = age.get();
    auto status =
        ApplySegmentUpdates(&segment,
                            20,
                            0,
                            {keys.data(), static_cast<int64_t>(keys.size())},
                            &field,
                            &literal,
                            1,
                            nullptr);
    EXPECT_EQ(status.error_code, FieldNotLoaded);
    if (status.error_code != Success)
        free(const_cast<char*>(status.error_msg));
    EXPECT_EQ(segment.GetUpdatesRecord().Stats(age).entries, 0);
}

TEST(PkUpdateSegment, RejectsTTLAndSiblingAtomically) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto age = schema->AddDebugField("age", DataType::INT64);
    auto ttl = schema->AddDebugField("expiry", DataType::TIMESTAMPTZ);
    schema->set_primary_field_id(pk);
    schema->set_ttl_field_id(ttl);
    UpdateAssignment expiry{ttl, {}};
    expiry.literal.mutable_scalar_value()->set_timestamptz_data(123);
    auto data = MakeRows(schema, pk, {1, 2});
    auto sealed = CreateSealedSegment(schema, nullptr, 87193);
    LoadGeneratedDataIntoSegment(data, sealed.get());
    auto growing = CreateGrowingSegment(schema, nullptr);
    growing->PreInsert(2);
    growing->Insert(
        0, 2, data.row_ids_.data(), data.timestamps_.data(), data.SharedRaw());
    auto check = [&](SegmentInternalInterface& segment) {
        EXPECT_THROW(
            segment.ApplyUpdates(Keys({42}), 10, 0, {Long(age, 9), expiry}),
            SegcoreError);
        EXPECT_EQ(segment.GetUpdatesRecord().Stats(age).entries, 0);
        EXPECT_EQ(segment.GetUpdatesRecord().Stats(ttl).entries, 0);
    };
    check(*growing);
    check(*sealed);
}

TEST(PkUpdateSegment, GCJoinsActiveNativeVisitor) {
    auto schema = std::make_shared<Schema>();
    const auto pk = schema->AddDebugField("pk", DataType::INT64);
    const auto age = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = MakeRows(schema, pk, {1});
    auto segment = CreateSealedSegment(schema, nullptr, 87194);
    LoadGeneratedDataIntoSegment(data, segment.get());
    const auto keys = Keys({42});
    segment->ApplyUpdates(keys, 10, 0, {Long(age, 100)});
    segment->ApplyUpdates(keys, 20, 0, {Long(age, 200)});
    const auto& record = segment->GetUpdatesRecord();

    std::promise<void> visiting, resume, collecting;
    auto resume_signal = resume.get_future();
    auto reader = std::async(std::launch::async, [&] {
        record.VisitUpdates(age, 10, [&](int64_t, const UpdateValue& value) {
            visiting.set_value();
            EXPECT_EQ(resume_signal.wait_for(std::chrono::seconds(5)),
                      std::future_status::ready);
            EXPECT_EQ(std::get<int64_t>(value), 100);
        });
    });
    ASSERT_EQ(visiting.get_future().wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    auto gc = std::async(std::launch::async, [&] {
        collecting.set_value();
        return GCUpdates(segment.get(), 20);
    });
    ASSERT_EQ(collecting.get_future().wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(gc.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    resume.set_value();
    reader.get();
    EXPECT_EQ(gc.get().error_code, Success);
    EXPECT_EQ(record.Stats(age).entries, 1);
    EXPECT_EQ(std::get<int64_t>(*record.Lookup(age, 0, 20)), 200);
}

TEST(PkUpdateSegment, SealedCApplyReplayAndReadFork) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto age = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = MakeRows(schema, pk, {1, 2, 20});
    auto segment = CreateSealedSegment(schema, nullptr, 87191);
    try {
        segment->ApplyUpdates(Keys({42}), 20, 3, {Long(age, 37)});
        FAIL() << "unavailable PK index silently consumed an update";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FieldNotLoaded);
    }
    EXPECT_EQ(segment->GetUpdatesRecord().Stats(age).entries, 0);
    LoadGeneratedDataIntoSegment(data, segment.get());
    auto fork =
        dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get())->PinReadState();
    int64_t field = age.get();
    auto ids = Keys({42}).SerializeAsString();
    auto literal = Long(age, 37).literal.SerializeAsString();
    CProto payload{literal.data(), static_cast<int64_t>(literal.size())};
    auto apply = [&](CLoadCancellationSource source) {
        return ApplySegmentUpdates(
            segment.get(),
            20,
            3,
            {ids.data(), static_cast<int64_t>(ids.size())},
            &field,
            &payload,
            1,
            source);
    };
    auto source = NewLoadCancellationSource();
    CancelLoadCancellationSource(source);
    auto status = apply(source);
    EXPECT_EQ(status.error_code, FollyCancel);
    free(const_cast<char*>(status.error_msg));
    ReleaseLoadCancellationSource(source);
    EXPECT_EQ(segment->GetUpdatesRecord().Stats(age).entries, 0);
    EXPECT_EQ(apply(nullptr).error_code, Success);
    EXPECT_EQ(apply(nullptr).error_code, Success);
    EXPECT_EQ(fork->GetUpdatesRecord().Stats(age).entries, 2);
    EXPECT_FALSE(fork->GetUpdatesRecord().Lookup(age, 2, 20));
    payload.proto_size = 0;
    status = apply(nullptr);
    EXPECT_EQ(status.error_code, DataFormatBroken);
    free(const_cast<char*>(status.error_msg));
    segment->SetCommitTimestamp(35);
    auto keys = Keys({42});
    segment->ApplyUpdates(keys, 35, 0, {Long(age, 1)});
    EXPECT_EQ(segment->GetUpdatesRecord().Stats(age).entries, 2);
    segment->ApplyUpdates(keys, 36, 0, {Long(age, 1)});
    EXPECT_EQ(segment->GetUpdatesRecord().Stats(age).entries, 5);
    auto invalid = GCUpdates(nullptr, 36);
    EXPECT_EQ(invalid.error_code, UnexpectedError);
    free(const_cast<char*>(invalid.error_msg));
    EXPECT_EQ(GCUpdates(segment.get(), 36).error_code, Success);
    EXPECT_EQ(fork->GetUpdatesRecord().Stats(age).entries, 3);
    EXPECT_EQ(std::get<int64_t>(*fork->GetUpdatesRecord().Lookup(age, 0, 36)),
              1);
}

}  // namespace
}  // namespace milvus::segcore
