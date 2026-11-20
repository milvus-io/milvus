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
            if (expected)
                EXPECT_EQ(std::get<int64_t>(*actual), *expected);
        }
    }
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
}

}  // namespace
}  // namespace milvus::segcore
