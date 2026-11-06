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

#include <algorithm>
#include <numeric>
#include <random>
#include <tuple>
#include <vector>

#include "common/FieldData.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::segcore {

class ChunkedDeleteTestAccess {
 public:
    static bool
    UseSortedSearch(const ChunkedSegmentSealedImpl& segment, bool load_replay) {
        return segment.UseSortedDeleteSearch(*segment.CapturePublishedState(),
                                             load_replay);
    }

    static std::vector<int64_t>
    LookupAtState(
        const ChunkedSegmentSealedImpl& segment,
        const std::shared_ptr<
            const ChunkedSegmentSealedImpl::PublishedSegmentState>& snapshot,
        const std::vector<PkType>& pks,
        Timestamp timestamp) {
        std::vector<int64_t> offsets;
        segment.search_batch_pks_from_state(
            snapshot,
            segment.UseSortedDeleteSearch(*snapshot, false),
            pks,
            [&](size_t) { return timestamp; },
            false,
            [&](SegOffset offset, Timestamp) {
                offsets.push_back(offset.get());
            });
        std::sort(offsets.begin(), offsets.end());
        return offsets;
    }
};

namespace {

class ChunkedDeleteTest
    : public testing::TestWithParam<std::tuple<bool, bool, size_t, size_t>> {
 protected:
    std::vector<int64_t> keys_{1, 7, 7, 7, 7, 9, 11, 11, 13};
    std::vector<Timestamp> insert_ts_{1, 5, 20, 25, 40, 3, 0, 50, 15};

    void
    SetUp() override {
        // Keep the duplicate versions across the first four chunks, then add
        // nonempty chunks to exercise both sides of the real column gates.
        for (size_t c = 4; c < std::get<3>(GetParam()); ++c) {
            keys_.push_back(100 + 2 * c);
            insert_ts_.push_back(1);
        }
    }

    PkType
    Key(int64_t value) const {
        if (std::get<0>(GetParam())) {
            return fmt::format("key_{:08d}", value);
        }
        return value;
    }

    IdArray
    Ids(const std::vector<int64_t>& keys) const {
        IdArray ids;
        for (auto key : keys) {
            if (std::get<0>(GetParam())) {
                ids.mutable_str_id()->add_data(std::get<std::string>(Key(key)));
            } else {
                ids.mutable_int_id()->add_data(key);
            }
        }
        return ids;
    }

    void
    ExpandBatch(std::vector<int64_t>& keys,
                std::vector<Timestamp>& timestamps) const {
        const auto original_keys = keys;
        const auto original_ts = timestamps;
        keys.resize(std::get<2>(GetParam()));
        timestamps.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            keys[i] = original_keys[i % original_keys.size()];
            timestamps[i] = original_ts[i % original_ts.size()];
        }
    }

    SegmentSealedUPtr
    MakeSegment(Timestamp commit_ts = 0) const {
        auto schema = std::make_shared<Schema>();
        const auto pk_type =
            std::get<0>(GetParam()) ? DataType::VARCHAR : DataType::INT64;
        auto pk = schema->AddDebugField("pk", pk_type);
        schema->set_primary_field_id(pk);
        auto segment = CreateSealedSegment(schema,
                                           nullptr,
                                           kSegmentID,
                                           SegcoreConfig::default_config(),
                                           std::get<1>(GetParam()));
        // PK 7 spans three chunks, with several insertion versions.
        std::vector<size_t> boundaries{0, 3, 4, 6, 9};
        while (boundaries.size() <= std::get<3>(GetParam())) {
            boundaries.push_back(boundaries.back() + 1);
        }
        auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        for (auto field : {RowFieldID, TimestampFieldID, pk}) {
            std::vector<FieldDataPtr> chunks;
            for (size_t c = 1; c < boundaries.size(); ++c) {
                const auto begin = boundaries[c - 1];
                const auto end = boundaries[c];
                if (field == pk && pk_type == DataType::VARCHAR) {
                    std::vector<std::string> values;
                    for (size_t i = begin; i < end; ++i) {
                        values.push_back(std::get<std::string>(Key(keys_[i])));
                    }
                    FieldDataPtr data =
                        std::make_shared<FieldData<std::string>>(
                            DataType::VARCHAR, false);
                    data->FillFieldData(values.data(), values.size());
                    chunks.push_back(std::move(data));
                } else {
                    std::vector<int64_t> values;
                    for (size_t i = begin; i < end; ++i) {
                        values.push_back(field == pk ? keys_[i]
                                         : field == TimestampFieldID
                                             ? insert_ts_[i]
                                             : i);
                    }
                    auto data = std::make_shared<FieldData<int64_t>>(
                        DataType::INT64, false);
                    data->FillFieldData(values.data(), values.size());
                    chunks.push_back(std::move(data));
                }
            }
            auto info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                       kPartitionID,
                                                       kSegmentID,
                                                       field.get(),
                                                       chunks,
                                                       cm);
            segment->LoadFieldData(info);
        }
        if (commit_ts != 0) {
            segment->SetCommitTimestamp(commit_ts);
        }
        const auto chunks = std::get<3>(GetParam());
        EXPECT_EQ(segment->num_chunk_data(pk), chunks);
        const auto& chunked =
            dynamic_cast<const ChunkedSegmentSealedImpl&>(*segment);
        EXPECT_EQ(ChunkedDeleteTestAccess::UseSortedSearch(chunked, false),
                  std::get<1>(GetParam()) && chunks >= 16);
        EXPECT_EQ(ChunkedDeleteTestAccess::UseSortedSearch(chunked, true),
                  std::get<1>(GetParam()) && chunks >= 32);
        return segment;
    }

    // Independent row scan: the earliest delete after each insertion wins,
    // regardless of the order within a batch or across replay/stream pushes.
    void
    ApplyOracle(std::vector<Timestamp>& deleted,
                const std::vector<int64_t>& keys,
                const std::vector<Timestamp>& timestamps,
                Timestamp commit_ts) const {
        for (size_t i = 0; i < keys.size(); ++i) {
            for (size_t row = 0; row < keys_.size(); ++row) {
                const auto inserted = commit_ts ? commit_ts : insert_ts_[row];
                if (keys_[row] == keys[i] && inserted < timestamps[i] &&
                    (deleted[row] == 0 || timestamps[i] < deleted[row])) {
                    deleted[row] = timestamps[i];
                }
            }
        }
    }

    void
    CheckMasks(SegmentSealed& segment,
               const std::vector<Timestamp>& deleted) const {
        for (Timestamp ts = 0; ts <= 110; ++ts) {
            BitsetType bitmap(keys_.size(), false);
            BitsetTypeView view(bitmap);
            segment.mask_with_delete(view, keys_.size(), ts);
            for (size_t row = 0; row < keys_.size(); ++row) {
                EXPECT_EQ(bitmap[row], deleted[row] != 0 && deleted[row] <= ts)
                    << "query_ts=" << ts << " row=" << row;
            }
        }
    }
};

TEST_P(ChunkedDeleteTest, BatchLookupMatchesScanWithOrderedAndUnorderedKeys) {
    for (Timestamp commit_ts : {0, 50}) {
        auto segment = MakeSegment(commit_ts);
        auto* chunked = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
        ASSERT_NE(chunked, nullptr);
        std::mt19937 rng(53901);
        std::vector<PkType> pks;
        for (size_t i = 0; i < std::get<2>(GetParam()); ++i) {
            pks.push_back(Key(rng() % 17));
        }
        for (bool sorted : {false, true}) {
            if (sorted) {
                std::sort(pks.begin(), pks.end());
            }
            for (bool include_same : {false, true}) {
                std::vector<Timestamp> timestamps(pks.size());
                for (auto& ts : timestamps) {
                    ts = rng() % 61;
                }
                std::vector<std::pair<int64_t, Timestamp>> expected, actual;
                for (size_t j = 0; j < pks.size(); ++j) {
                    for (size_t row = 0; row < keys_.size(); ++row) {
                        const auto inserted =
                            commit_ts ? commit_ts : insert_ts_[row];
                        if (Key(keys_[row]) == pks[j] &&
                            (inserted < timestamps[j] ||
                             (include_same && inserted == timestamps[j]))) {
                            expected.emplace_back(row, timestamps[j]);
                        }
                    }
                }
                const auto original = pks;
                chunked->search_batch_pks(
                    pks,
                    [&](size_t i) { return timestamps[i]; },
                    include_same,
                    [&](SegOffset offset, Timestamp ts) {
                        actual.emplace_back(offset.get(), ts);
                    });
                std::sort(actual.begin(), actual.end());
                std::sort(expected.begin(), expected.end());
                EXPECT_EQ(actual, expected);
                EXPECT_EQ(pks, original);
            }
        }

        // Publishing another commit timestamp must not change a lookup that
        // was bound to the previous state before preparing the delete batch.
        const auto captured = chunked->TestGetPublishedStateSnapshot();
        segment->SetCommitTimestamp(200);
        const std::vector<PkType> one_pk{Key(7)};
        EXPECT_EQ(ChunkedDeleteTestAccess::LookupAtState(
                      *chunked, captured, one_pk, 70),
                  (std::vector<int64_t>{1, 2, 3, 4}));
        size_t current_hits = 0;
        chunked->search_batch_pks(
            one_pk,
            [](size_t) { return Timestamp{70}; },
            false,
            [&](SegOffset, Timestamp) { ++current_hits; });
        EXPECT_EQ(current_hits, 0);
    }
}

TEST_P(ChunkedDeleteTest, StreamingDeletesPreserveVersionedMasks) {
    for (Timestamp commit_ts : {0, 50}) {
        auto segment = MakeSegment(commit_ts);
        std::vector<int64_t> keys{13, 7, 11, 1, 7, 7, 9, 7, 99};
        std::vector<Timestamp> timestamps{15, 30, 50, 1, 10, 50, 3, 20, 80};
        ExpandBatch(keys, timestamps);
        auto ids = Ids(keys);
        const auto original = ids.SerializeAsString();
        ASSERT_TRUE(segment->Delete(keys.size(), &ids, timestamps.data()).ok());
        EXPECT_EQ(ids.SerializeAsString(), original);
        std::vector<Timestamp> deleted(keys_.size());
        ApplyOracle(deleted, keys, timestamps, commit_ts);
        CheckMasks(*segment, deleted);

        const std::vector<int64_t> more_keys{7, 11, 13, 1, 9};
        const std::vector<Timestamp> more_ts{60, 60, 60, 60, 60};
        ids = Ids(more_keys);
        ASSERT_TRUE(
            segment->Delete(more_keys.size(), &ids, more_ts.data()).ok());
        ApplyOracle(deleted, more_keys, more_ts, commit_ts);
        CheckMasks(*segment, deleted);
    }
}

TEST_P(ChunkedDeleteTest,
       LoadReplayKeepsEarliestDeleteBeforeAndAfterStreaming) {
    for (Timestamp commit_ts : {0, 50}) {
        auto segment = MakeSegment(commit_ts);
        // Replay order must not hide the earlier effective deletes of PK 7:
        // each inserted version picks the earliest applicable timestamp.
        std::vector<int64_t> keys{13, 7, 11, 7, 1, 7, 99};
        std::vector<Timestamp> timestamps{15, 80, 50, 60, 0, 20, 100};
        ExpandBatch(keys, timestamps);
        auto ids = Ids(keys);
        const auto original = ids.SerializeAsString();
        LoadDeletedRecordInfo info{
            timestamps.data(), &ids, static_cast<int64_t>(keys.size())};
        segment->LoadDeletedRecord(info);
        EXPECT_EQ(ids.SerializeAsString(), original);
        std::vector<Timestamp> deleted(keys_.size());
        ApplyOracle(deleted, keys, timestamps, commit_ts);
        CheckMasks(*segment, deleted);

        const std::vector<int64_t> more_keys{11, 1, 13, 9, 7};
        const std::vector<Timestamp> more_ts{100, 90, 95, 100, 100};
        ids = Ids(more_keys);
        ASSERT_TRUE(
            segment->Delete(more_keys.size(), &ids, more_ts.data()).ok());
        ApplyOracle(deleted, more_keys, more_ts, commit_ts);
        CheckMasks(*segment, deleted);

        // Older replay after streaming must move the effective timestamp
        // backwards without counting the row as deleted twice.
        const std::vector<int64_t> older_keys{7, 1, 7, 13, 11, 9};
        const std::vector<Timestamp> older_ts{55, 65, 35, 60, 70, 60};
        ids = Ids(older_keys);
        LoadDeletedRecordInfo older_info{
            older_ts.data(), &ids, static_cast<int64_t>(older_keys.size())};
        segment->LoadDeletedRecord(older_info);
        ApplyOracle(deleted, older_keys, older_ts, commit_ts);
        CheckMasks(*segment, deleted);
        const auto expected_count =
            std::count_if(deleted.begin(), deleted.end(), [](Timestamp ts) {
                return ts != 0;
            });
        EXPECT_EQ(segment->get_deleted_count(), expected_count);
        segment->LoadDeletedRecord(older_info);
        CheckMasks(*segment, deleted);
        EXPECT_EQ(segment->get_deleted_count(), expected_count);
    }
}

INSTANTIATE_TEST_SUITE_P(Int64AndVarchar,
                         ChunkedDeleteTest,
                         testing::Combine(testing::Bool(),
                                          testing::Bool(),
                                          testing::Values(size_t{1},
                                                          size_t{10},
                                                          size_t{100},
                                                          size_t{500},
                                                          size_t{2048}),
                                          testing::Values(size_t{15},
                                                          size_t{16},
                                                          size_t{17},
                                                          size_t{31},
                                                          size_t{32},
                                                          size_t{33})));

}  // namespace
}  // namespace milvus::segcore
