// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "segcore/UpdatesRecordReaders.h"

namespace milvus::segcore::experimental {
namespace {
const auto kResolve = [](const std::vector<UpdateValue>& keys, uint64_t) {
    std::vector<int64_t> offsets;
    for (const auto& key : keys) offsets.push_back(std::get<int64_t>(key));
    return offsets;
};

TEST(TsVectorUpdates, ReverseCorrectionDirectoryAndHistoricalSnapshots) {
    constexpr size_t rows = 16403;
    UpdatesRecord records(rows);
    records.RegisterField(
        1, UpdateFieldType::Int64, true, [](const auto& offsets) {
            UpdatesRecord::Values result;
            for (auto offset : offsets)
                result.push_back(std::make_shared<const UpdateValue>(offset));
            return result;
        });
    struct Mutation {
        int64_t offset;
        uint64_t ts;
        UpdateValue value;
    };
    const std::vector<Mutation> mutations = {{10, 100, int64_t{18}},
                                             {8191, 100, int64_t{7}},
                                             {8192, 100, int64_t{42}},
                                             {10, 101, int64_t{20}},
                                             {8192, 101, std::monostate{}},
                                             {10, 102, int64_t{18}},
                                             {16402, 102, int64_t{0}}};
    for (size_t i = 0; i < mutations.size(); ++i) {
        const auto& m = mutations[i];
        ASSERT_EQ(records.Apply({m.offset}, m.ts, {{1, m.value}}, kResolve), 1);
        if (i == 2) {
            for (uint64_t ts : {99, 100}) {
                size_t selected = 0;
                records.VisitUpdates(
                    1, ts, [&](int64_t, const auto&) { ++selected; });
                EXPECT_EQ(selected, ts == 99 ? 0 : 3);
            }
            EXPECT_EQ(records.GetStats().ts_unique_reads, 2);
            EXPECT_EQ(records.GetStats().ts_visited_bitmap_bytes, 0);
        }
    }
    EXPECT_EQ(records.GetStats().updated_cells, 4);
    EXPECT_EQ(records.GetStats().update_versions, 7);
    EXPECT_EQ(records.GetStats().directory_refs, 7);
    const std::vector<int64_t> offsets{10, 8191, 8192, 16402, 10, 11};
    for (uint64_t ts : {0, 99, 100, 101, 102, 1000}) {
        SCOPED_TRACE(ts);
        std::vector<UpdateValue> expected(rows);
        for (size_t i = 0; i < rows; ++i) expected[i] = int64_t(i);
        for (const auto& m : mutations)
            if (m.ts <= ts)
                expected[m.offset] = m.value;
        TargetBitmap result(rows, false), valid(rows, true);
        result.set(18);
        const auto before = records.GetStats();
        records.CorrectEquality(1, ts, int64_t{18}, true, result, &valid);
        const auto after = records.GetStats();
        EXPECT_EQ(after.ts_entries_visited - before.ts_entries_visited,
                  ts < 100    ? 0
                  : ts == 100 ? 3
                  : ts == 101 ? 5
                              : 6);
        for (size_t i = 0; i < rows; ++i) {
            ASSERT_EQ(bool(valid[i]),
                      !std::holds_alternative<std::monostate>(expected[i]));
            ASSERT_EQ(bool(result[i]), expected[i] == UpdateValue(int64_t{18}));
        }
        auto reader = records.CreateReader(1, ts);
        auto gathered = reader->ReadOffsets(offsets);
        for (size_t i = 0; i < offsets.size(); ++i)
            EXPECT_EQ(*gathered[i], expected[offsets[i]]);
        for (auto [begin, count] : std::vector<std::pair<int64_t, size_t>>{
                 {0, 8092}, {8092, 8092}, {16184, 219}}) {
            TargetBitmap candidates(count + 3, false);
            for (size_t i = 0; i < count; ++i)
                candidates[i + 3] = (begin + i) % 5 != 1;
            auto mask = candidates.view();
            auto patches = reader->ReadRangeUpdates(begin, count, &mask, 3);
            std::vector<UpdateValue> batch(count);
            for (size_t i = 0; i < count; ++i) batch[i] = begin + int64_t(i);
            for (const auto& patch : patches) {
                ASSERT_LT(patch.position, count);
                ASSERT_TRUE(candidates[patch.position + 3]);
                batch[patch.position] = *patch.value;
            }
            for (size_t i = 0; i < count; ++i)
                if (candidates[i + 3])
                    ASSERT_EQ(batch[i], expected[begin + i]);
        }
    }
    auto pinned = records.CreateReader(1, 102);
    records.Apply({int64_t{10}}, 103, {{1, int64_t{99}}}, kResolve);
    EXPECT_EQ(std::get<int64_t>(*pinned->ReadOffsets({10}).front()), 18);
    EXPECT_GT(records.GetStats().lookup_history, 0);
    EXPECT_GT(records.GetStats().lookup_latest, 0);
}

TEST(TsVectorUpdates, StringOwnershipAndReaderSurvivesWriter) {
    std::unique_ptr<ValueReader> reader;
    const std::string original(300, 'x');
    {
        UpdatesRecord records(64);
        records.RegisterField(
            2, UpdateFieldType::VarChar, true, [](const auto& rows) {
                return UpdatesRecord::Values(
                    rows.size(),
                    std::make_shared<const UpdateValue>(std::string("base")));
            });
        records.Apply({int64_t{1}}, 100, {{2, original}}, kResolve);
        records.Apply({int64_t{2}}, 100, {{2, std::string{}}}, kResolve);
        records.Apply({int64_t{3}}, 100, {{2, std::monostate{}}}, kResolve);
        reader = records.CreateReader(2, 100);
        auto retained = reader->ReadOffsets({1, 2, 3});
        for (int64_t row = 4; row < 64; ++row)
            records.Apply({row}, 101, {{2, std::string(400, 'y')}}, kResolve);
        records.Apply({int64_t{1}}, 102, {{2, std::string("new")}}, kResolve);
        EXPECT_EQ(std::get<std::string>(*retained[0]), original);
        EXPECT_EQ(std::get<std::string>(*retained[1]), "");
        EXPECT_TRUE(std::holds_alternative<std::monostate>(*retained[2]));
    }
    auto values = reader->ReadOffsets({1, 3, 0});
    EXPECT_EQ(std::get<std::string>(*values[0]), original);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(*values[1]));
    EXPECT_EQ(std::get<std::string>(*values[2]), "base");
    const int64_t output_offsets[] = {1, -1, 3};
    auto output = reader->ReadOutput(output_offsets, 3, true);
    EXPECT_EQ(output->scalars().string_data().data(0), original);
    EXPECT_EQ(std::vector<bool>(output->valid_data().begin(),
                                output->valid_data().end()),
              (std::vector<bool>{true, false, false}));
}

TEST(TsVectorUpdates, InvalidApplyDoesNotPublishPartialMutation) {
    UpdatesRecord records(20);
    auto base = [](const auto& rows) {
        return UpdatesRecord::Values(
            rows.size(), std::make_shared<const UpdateValue>(int64_t{0}));
    };
    records.RegisterField(1, UpdateFieldType::Int64, true, base);
    records.RegisterField(2, UpdateFieldType::Int64, false, base);
    records.RegisterField(3, UpdateFieldType::Int64, false, base);
    ASSERT_EQ(records.Apply({int64_t{1}, int64_t{1}},
                            100,
                            {{1, int64_t{10}}, {2, int64_t{11}}},
                            kResolve),
              1);
    EXPECT_THROW(records.Apply({int64_t{2}}, 99, {{1, int64_t{20}}}, kResolve),
                 SegcoreError);
    EXPECT_THROW(records.Apply({int64_t{2}},
                               101,
                               {{1, int64_t{20}}, {2, std::monostate{}}},
                               kResolve),
                 SegcoreError);
    EXPECT_THROW(
        records.Apply({int64_t{20}}, 101, {{1, int64_t{20}}}, kResolve),
        SegcoreError);
    // Allocation/validation preparation may create an empty directory entry,
    // but another field's timestamp conflict must publish neither assignment.
    EXPECT_THROW(
        records.Apply(
            {int64_t{1}}, 100, {{3, int64_t{20}}, {1, int64_t{20}}}, kResolve),
        SegcoreError);
    EXPECT_EQ(records.GetStats().update_versions, 2);
    EXPECT_EQ(records.GetStats().directory_refs, 2);
    EXPECT_EQ(*records.Lookup(1, 2, 200), UpdateValue(int64_t{0}));
    EXPECT_EQ(*records.Lookup(3, 1, 200), UpdateValue(int64_t{0}));
    EXPECT_EQ(records.Apply({int64_t{2}}, 100, {{1, int64_t{20}}}, kResolve),
              1);
}
}  // namespace
}  // namespace milvus::segcore::experimental
