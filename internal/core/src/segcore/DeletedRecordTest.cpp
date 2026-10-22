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
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <folly/ScopeGuard.h>

#include "bitset/bitset.h"
#include "common/Common.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/function/FunctionFactory.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "query/Utils.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/DeletedRecord.h"
#include "segcore/InsertRecord.h"
#include "segcore/Record.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::exec;

TEST(DeleteMVCC, common_case) {
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    factory.Initialize();
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);

    // load insert:     pk (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    // with timestamp   ts (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    int64_t c = 10;
    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    auto segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    ASSERT_EQ(c, segment->get_real_count());
    DeletedRecord<true> delete_record(
        nullptr,
        [&pks](const std::vector<PkType>& delete_pks,
               const Timestamp* timestamps,
               std::function<void(const SegOffset offset, const Timestamp ts)>
                   cb) {
            for (size_t i = 0; i < delete_pks.size(); ++i) {
                auto delete_pk = std::get<int64_t>(delete_pks[i]);
                auto delete_ts = timestamps[i];
                for (size_t offset = 0; offset < pks.size(); ++offset) {
                    if (pks[offset] == delete_pk &&
                        static_cast<Timestamp>(offset) < delete_ts) {
                        cb(SegOffset(offset), delete_ts);
                    }
                }
            }
        },
        0);
    delete_record.set_sealed_row_count(c);

    // delete pk(1) at ts(10);
    std::vector<Timestamp> delete_ts = {10};
    std::vector<PkType> delete_pk = {1};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(1, delete_record.size());

    {
        BitsetType bitsets(c);
        auto bitsets_view = bitsets.write_view();
        auto insert_barrier = c;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }
    {
        BitsetType bitsets(c);
        auto bitsets_view = bitsets.write_view();
        auto insert_barrier = c;
        // query at ts (11)
        Timestamp query_timestamp = 11;
        // query at ts (11)
        query_timestamp = 11;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }

    // delete pk(5) at ts(12)
    delete_ts = {12};
    delete_pk = {5};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(2, delete_record.size());

    {
        BitsetType bitsets(c);
        auto bitsets_view = bitsets.write_view();
        auto insert_barrier = c;
        // query at ts (12)
        Timestamp query_timestamp = 12;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }

    // delete at pk(1) at ts(13) again
    delete_ts = {13};
    delete_pk = {1};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    // not add new record, because already deleted.
    ASSERT_EQ(2, delete_record.size());

    {
        BitsetType bitsets(c);
        auto bitsets_view = bitsets.write_view();
        auto insert_barrier = c;
        // query at ts (14)
        Timestamp query_timestamp = 14;

        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }

    // delete pk(9) at ts(9)
    delete_ts = {9};
    delete_pk = {9};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    // not add new record, because insert also at ts(9) same as deleted
    // delete not take effect.
    ASSERT_EQ(2, delete_record.size());

    {
        BitsetType bitsets(c);
        auto bitsets_view = bitsets.write_view();
        auto insert_barrier = c;
        // query at ts (14)
        Timestamp query_timestamp = 14;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }
}

TEST(DeleteMVCC, normal_caller_filters_delete_before_or_at_insert_timestamp) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);

    InsertRecord<false> insert_record(*schema, 4);
    DeletedRecord<false> delete_record(
        &insert_record,
        [&insert_record](
            const std::vector<PkType>& pks,
            const Timestamp* timestamps,
            std::function<void(const SegOffset offset, const Timestamp ts)>
                cb) {
            for (size_t i = 0; i < pks.size(); ++i) {
                auto timestamp = timestamps[i];
                auto offsets =
                    insert_record.search_pk(pks[i], timestamp, false);
                for (auto offset : offsets) {
                    cb(offset, timestamp);
                }
            }
        },
        0);

    std::vector<int64_t> pk_data = {10};
    std::vector<Timestamp> insert_ts = {100};
    insert_record.insert_pk(pk_data[0], 0);
    auto insert_offset = insert_record.reserved.fetch_add(1);
    insert_record.timestamps_.set_data_raw(insert_offset, insert_ts.data(), 1);
    auto field_data = insert_record.get_data_base(pk);
    field_data->set_data_raw(insert_offset, pk_data.data(), 1);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + 1);

    std::vector<PkType> delete_pk = {pk_data[0], pk_data[0]};
    std::vector<Timestamp> delete_ts = {99, 100};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(0, delete_record.size());

    delete_pk = {pk_data[0]};
    delete_ts = {101};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(1, delete_record.size());
}

TEST(DeleteMVCC, delete_exist_duplicate_pks) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
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

    // insert pk: (0, 1, 1, 2, 2, 3, 4, 3, 2, 5)
    // at ts:     (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    std::vector<int64_t> age_data = {0, 1, 1, 2, 2, 3, 4, 3, 2, 5};
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        tss[i] = i;
        insert_record.insert_pk(age_data[i], i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // delete pk(2) at ts(5)
    std::vector<Timestamp> delete_ts = {5};
    std::vector<PkType> delete_pk = {2};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(2, delete_record.size());

    {
        BitsetType bitsets(N);
        auto bitsets_view = bitsets.write_view();
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 0, 0, 1, 1, 0, 0, 0, 0, 0};
        // two pk 2  at ts(3, 4) was deleted
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }

    // delete pk(3) at ts(6)
    delete_ts = {6};
    delete_pk = {3};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(3, delete_record.size());

    {
        BitsetType bitsets(N);
        auto bitsets_view = bitsets.write_view();
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 0, 0, 1, 1, 1, 0, 0, 0, 0};
        // one pk 3 in ts(5) was deleted
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }

    // delete pk(3) at ts(9) again
    delete_ts = {9};
    delete_pk = {3};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(4, delete_record.size());

    {
        BitsetType bitsets(N);
        auto bitsets_view = bitsets.write_view();
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 0, 0, 1, 1, 1, 0, 1, 0, 0};
        //  pk 3 in ts(7) was deleted
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }

    // delete pk(2) at ts(9) again
    delete_ts = {9};
    delete_pk = {2};
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(5, delete_record.size());

    {
        BitsetType bitsets(N);
        auto bitsets_view = bitsets.write_view();
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 0, 0, 1, 1, 1, 0, 1, 1, 0};
        //  pk 2 in ts(8) was deleted
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }
}

TEST(DeleteMVCC, snapshot) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 50000;
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

    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = i;
        tss[i] = i;
        insert_record.insert_pk(age_data[i], i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    auto DN = 40000;
    std::vector<Timestamp> delete_ts(DN);
    std::vector<PkType> delete_pk(DN);
    for (int i = 0; i < DN; ++i) {
        delete_pk[i] = age_data[i];
        delete_ts[i] = i + 1;
    }
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(DN, delete_record.size());

    auto snapshots = delete_record.get_snapshots();
    ASSERT_EQ(3, snapshots.size());
    ASSERT_EQ(snapshots[2].second.count(), 30000);
}

TEST(DeleteMVCC, insert_after_snapshot) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 11000;
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

    // insert (0, 0), (1, 1) .... (N - 1, N - 1)
    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = i;
        tss[i] = i;
        insert_record.insert_pk(age_data[i], i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // delete (0, 1), (1, 2) .... (DN, DN + 1)
    auto DN = 10100;
    std::vector<Timestamp> delete_ts(DN);
    std::vector<PkType> delete_pk(DN);
    for (int i = 0; i < DN; ++i) {
        delete_pk[i] = age_data[i];
        delete_ts[i] = i + 1;
    }
    delete_record.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(DN, delete_record.size());

    auto snapshots = delete_record.get_snapshots();
    ASSERT_EQ(1, snapshots.size());
    ASSERT_EQ(snapshots[0].second.count(), 10000);

    // Query at N+1 ts
    {
        BitsetType bitsets(N);
        auto bitsets_view = bitsets.write_view();
        int64_t insert_barrier = N;
        Timestamp query_timestamp = N + 1;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        for (auto i = 0; i < DN; i++) {
            ASSERT_EQ(bitsets_view[i], true) << i;
        }
        for (auto i = DN; i < N; i++) {
            ASSERT_EQ(bitsets_view[i], false) << i;
        }
    }

    // insert (N, N), (N + 1, N + 1).... (N + AN - 1, N + AN - 1) again
    auto AN = 1000;
    std::vector<int64_t> age_data_new(AN);
    std::vector<Timestamp> tss_new(AN);
    for (int i = 0; i < AN; ++i) {
        age_data_new[i] = N + i;
        tss_new[i] = N + i;
        insert_record.insert_pk(age_data_new[i], i + N);
    }
    insert_offset = insert_record.reserved.fetch_add(AN);
    insert_record.timestamps_.set_data_raw(insert_offset, tss_new.data(), AN);
    field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), AN);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + AN);

    // Query at N + AN + 1 ts
    {
        BitsetType bitsets(N + AN);
        auto bitsets_view = bitsets.write_view();
        int64_t insert_barrier = N + AN;
        Timestamp query_timestamp = N + AN + 1;
        delete_record.Query(bitsets, insert_barrier, query_timestamp);
        for (auto i = 0; i < DN; i++) {
            ASSERT_EQ(bitsets_view[i], true);
        }
        for (auto i = DN; i < N + AN; i++) {
            ASSERT_EQ(bitsets_view[i], false);
        }
    }
}

TEST(DeleteMVCC, perform) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 1000000;
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

    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = i;
        tss[i] = i;
        insert_record.insert_pk(i, i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    auto DN = N / 2;
    std::vector<Timestamp> delete_ts(DN);
    std::vector<PkType> delete_pk(DN);
    for (int i = 0; i < DN; ++i) {
        delete_ts[i] = N + i;
        delete_pk[i] = i;
    }
    auto start = std::chrono::steady_clock::now();
    delete_record.StreamPush(delete_pk, delete_ts.data());
    auto end = std::chrono::steady_clock::now();
    std::cout << "push cost:"
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << std::endl;
    std::cout << delete_record.size() << std::endl;

    auto query_timestamp = delete_ts[DN - 1];
    auto insert_barrier = get_barrier(insert_record, query_timestamp);
    BitsetType res_bitmap(insert_barrier);
    auto res_view = res_bitmap.write_view();
    start = std::chrono::steady_clock::now();
    delete_record.Query(res_bitmap, insert_barrier, query_timestamp);
    end = std::chrono::steady_clock::now();
    std::cout << "query cost:"
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << std::endl;
}

TEST(DeleteMVCC, QueryTimestampLowerThanFirstSnapshot) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 50000;
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

    // insert (0,0), (1,1), ..., (N-1,N-1)
    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = i;
        tss[i] = i;
        insert_record.insert_pk(age_data[i], i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // delete first DN pks with ts = i+1, ensure snapshots are created
    auto DN = 40000;
    std::vector<Timestamp> delete_ts(DN);
    std::vector<PkType> delete_pk(DN);
    for (int i = 0; i < DN; ++i) {
        delete_pk[i] = age_data[i];
        delete_ts[i] = i + 1;  // 1..DN
    }
    delete_record.StreamPush(delete_pk, delete_ts.data());

    auto snapshots = delete_record.get_snapshots();
    std::cout << "snapshots size: " << snapshots.size() << std::endl;
    ASSERT_GE(snapshots.size(), 1);

    // Query at ts smaller than first snapshot ts, expect only first deletion visible
    Timestamp query_timestamp = 1;
    BitsetType bitsets(N);
    auto bitsets_view = bitsets.write_view();
    int64_t insert_barrier = N;
    ASSERT_EQ(bitsets_view.count(), 0);
    ASSERT_TRUE(bitsets_view.none());
    delete_record.Query(bitsets, insert_barrier, query_timestamp);
    EXPECT_EQ(bitsets_view.count(), 1);
    EXPECT_FALSE(bitsets_view.none());

    for (int i = 0; i < N; i++) {
        bool expected = (i == 0);
        ASSERT_EQ(bitsets_view[i], expected) << i;
    }
    bitsets.reset(0);
    EXPECT_EQ(bitsets_view.count(), 0);
    EXPECT_TRUE(bitsets_view.none());
}

TEST(DeleteMVCC, LatestSnapshotOptimizationBenchmark) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    const int N = 100000;  // total rows
    const int DN = 9000;   // delete count (< 10000 to avoid snapshot dump)
    const int QUERY_COUNT = 10000;  // number of queries

    // Helper lambda to create search_pk function
    auto make_search_pk_func = [](InsertRecord<false>& insert_record) {
        return [&insert_record](const std::vector<PkType>& pks,
                                const Timestamp* timestamps,
                                std::function<void(const SegOffset offset,
                                                   const Timestamp ts)> cb) {
            for (size_t i = 0; i < pks.size(); ++i) {
                auto timestamp = timestamps[i];
                auto offsets = insert_record.search_pk(pks[i], timestamp);
                for (auto offset : offsets) {
                    cb(offset, timestamp);
                }
            }
        };
    };

    // Helper lambda to setup insert record
    auto setup_insert_record = [&](InsertRecord<false>& insert_record,
                                   std::vector<int64_t>& age_data,
                                   std::vector<Timestamp>& tss) {
        for (int i = 0; i < N; ++i) {
            age_data[i] = i;
            tss[i] = i;
            insert_record.insert_pk(age_data[i], i);
        }
        auto insert_offset = insert_record.reserved.fetch_add(N);
        insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
        auto field_data = insert_record.get_data_base(i64_fid);
        field_data->set_data_raw(insert_offset, age_data.data(), N);
        insert_record.ack_responder_.AddSegment(insert_offset,
                                                insert_offset + N);
    };

    // Prepare delete data
    std::vector<Timestamp> delete_ts(DN);
    std::vector<PkType> delete_pk(DN);
    for (int i = 0; i < DN; ++i) {
        delete_pk[i] = i;      // delete first DN pks
        delete_ts[i] = N + i;  // ts after all inserts
    }

    Timestamp query_timestamp = N + DN + 100;  // after all deletes
    int64_t insert_barrier = N;

    // ============ Setup for Optimization OFF (no latest_snapshot_) ============
    InsertRecord<false> insert_record_off(*schema, N);
    DeletedRecord<false> delete_record_off(
        &insert_record_off, make_search_pk_func(insert_record_off), 0);

    std::vector<int64_t> age_data_off(N);
    std::vector<Timestamp> tss_off(N);
    setup_insert_record(insert_record_off, age_data_off, tss_off);

    // Push deletes with optimization OFF - no latest_snapshot_ created
    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(false);
    delete_record_off.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(DN, delete_record_off.size());

    // ============ Setup for Optimization ON (has latest_snapshot_) ============
    InsertRecord<false> insert_record_on(*schema, N);
    DeletedRecord<false> delete_record_on(
        &insert_record_on, make_search_pk_func(insert_record_on), 0);

    std::vector<int64_t> age_data_on(N);
    std::vector<Timestamp> tss_on(N);
    setup_insert_record(insert_record_on, age_data_on, tss_on);

    // Push deletes with optimization ON - latest_snapshot_ created
    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(true);
    delete_record_on.StreamPush(delete_pk, delete_ts.data());
    ASSERT_EQ(DN, delete_record_on.size());

    // ============ Verify correctness ============
    BitsetType ref_bitmap(insert_barrier);
    auto ref_view = ref_bitmap.write_view();
    delete_record_off.Query(ref_bitmap, insert_barrier, query_timestamp);

    BitsetType fast_bitmap(insert_barrier);
    auto fast_view = fast_bitmap.write_view();
    delete_record_on.Query(fast_bitmap, insert_barrier, query_timestamp);

    // Verify both produce same result
    for (int i = 0; i < DN; ++i) {
        ASSERT_TRUE(ref_view[i]) << "Row " << i << " should be deleted (OFF)";
        ASSERT_TRUE(fast_view[i]) << "Row " << i << " should be deleted (ON)";
    }
    for (int i = DN; i < N; ++i) {
        ASSERT_FALSE(ref_view[i])
            << "Row " << i << " should NOT be deleted (OFF)";
        ASSERT_FALSE(fast_view[i])
            << "Row " << i << " should NOT be deleted (ON)";
    }
    std::cout << "Correctness verified: OFF and ON produce same results"
              << std::endl;

    // ============ Benchmark: Optimization OFF (slow path) ============
    BitsetType res_bitmap(insert_barrier);
    auto start_off = std::chrono::steady_clock::now();
    for (int i = 0; i < QUERY_COUNT; ++i) {
        res_bitmap.reset();
        auto res_view = res_bitmap.write_view();
        delete_record_off.Query(res_bitmap, insert_barrier, query_timestamp);
    }
    auto end_off = std::chrono::steady_clock::now();
    auto duration_off = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_off - start_off)
                            .count();
    std::cout << "[OFF] result bitmap count: " << res_bitmap.count()
              << std::endl;

    // ============ Benchmark: Optimization ON (fast path) ============
    auto start_on = std::chrono::steady_clock::now();
    for (int i = 0; i < QUERY_COUNT; ++i) {
        res_bitmap.reset();
        auto res_view = res_bitmap.write_view();
        delete_record_on.Query(res_bitmap, insert_barrier, query_timestamp);
    }
    auto end_on = std::chrono::steady_clock::now();
    auto duration_on =
        std::chrono::duration_cast<std::chrono::microseconds>(end_on - start_on)
            .count();
    std::cout << "[ON]  result bitmap count: " << res_bitmap.count()
              << std::endl;

    // ============ Print results ============
    std::cout << "============================================" << std::endl;
    std::cout << "Latest Delete Snapshot Optimization Benchmark" << std::endl;
    std::cout << "============================================" << std::endl;
    std::cout << "Config: N=" << N << ", Deleted=" << DN
              << ", Queries=" << QUERY_COUNT << std::endl;
    std::cout << "--------------------------------------------" << std::endl;
    std::cout << "Optimization OFF (slow path): " << duration_off << " us ("
              << duration_off / QUERY_COUNT << " us/query)" << std::endl;
    std::cout << "Optimization ON  (fast path): " << duration_on << " us ("
              << duration_on / QUERY_COUNT << " us/query)" << std::endl;
    std::cout << "--------------------------------------------" << std::endl;

    if (duration_on < duration_off) {
        double speedup = static_cast<double>(duration_off) / duration_on;
        std::cout << "Speedup: " << std::fixed << std::setprecision(2)
                  << speedup << "x faster with optimization ON" << std::endl;
    } else {
        double slowdown = static_cast<double>(duration_on) / duration_off;
        std::cout << "WARNING: " << std::fixed << std::setprecision(2)
                  << slowdown << "x slower with optimization ON" << std::endl;
    }
    std::cout << "============================================" << std::endl;

    // Verify optimization is faster
    EXPECT_LT(duration_on, duration_off)
        << "Optimization should be faster than slow path";

    // Restore default
    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(true);
}

TEST(DeleteMVCC, SnapshotDumpProgress) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    const int N = 21000;
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

    // insert pk=i at ts=i
    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = i;
        tss[i] = i;
        insert_record.insert_pk(age_data[i], i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // 1) Push exactly 10000 deletes -> no snapshot expected (threshold is > 10000)
    const int B = 10000;  // current DUMP_BATCH_SIZE
    std::vector<Timestamp> delete_ts1(B);
    std::vector<PkType> delete_pk1(B);
    for (int i = 0; i < B; ++i) {
        delete_pk1[i] = age_data[i];
        delete_ts1[i] = i + 1;
    }
    delete_record.StreamPush(delete_pk1, delete_ts1.data());
    auto snapshots = delete_record.get_snapshots();
    ASSERT_EQ(0, snapshots.size());

    // 2) Push one more delete -> one snapshot expected, covering first 10000
    std::vector<Timestamp> delete_ts2(1);
    std::vector<PkType> delete_pk2(1);
    delete_pk2[0] = age_data[B];
    delete_ts2[0] = B + 1;  // 10001
    delete_record.StreamPush(delete_pk2, delete_ts2.data());
    snapshots = delete_record.get_snapshots();
    ASSERT_EQ(1, snapshots.size());
    ASSERT_EQ(10000, snapshots[0].second.count());

    // 3) Push 9500 more -> still one snapshot
    const int more1 = 9500;
    std::vector<Timestamp> delete_ts3(more1);
    std::vector<PkType> delete_pk3(more1);
    for (int i = 0; i < more1; ++i) {
        delete_pk3[i] = age_data[B + 1 + i];
        delete_ts3[i] = B + 2 + i;
    }
    delete_record.StreamPush(delete_pk3, delete_ts3.data());
    snapshots = delete_record.get_snapshots();
    ASSERT_EQ(1, snapshots.size());
    ASSERT_EQ(10000, snapshots[0].second.count());

    // 4) Push 500 more (total 10001 after previous 10000 dumped) -> second snapshot appears
    const int more2 = 500;
    std::vector<Timestamp> delete_ts4(more2);
    std::vector<PkType> delete_pk4(more2);
    for (int i = 0; i < more2; ++i) {
        delete_pk4[i] = age_data[B + 1 + more1 + i];
        delete_ts4[i] = B + 2 + more1 + i;
    }
    delete_record.StreamPush(delete_pk4, delete_ts4.data());
    snapshots = delete_record.get_snapshots();
    ASSERT_EQ(2, snapshots.size());
    ASSERT_EQ(20000, snapshots[1].second.count());
}

namespace {

// Builds a sealed DeletedRecord whose search callback mirrors production:
// a delete reaches a row only when the row was inserted strictly before it.
// With `reverse_matches`, rows are reported from the last one to the first,
// each row's matches still in input order, like the chunk-by-chunk search of
// a sealed segment may do.
// `pks` and `insert_tss` must outlive the returned record.
std::unique_ptr<DeletedRecord<true>>
MakeSealedDeletedRecord(const std::vector<int64_t>& pks,
                        const std::vector<Timestamp>& insert_tss,
                        bool reverse_matches = false) {
    auto record = std::make_unique<DeletedRecord<true>>(
        nullptr,
        [&pks, &insert_tss, reverse_matches](
            const std::vector<PkType>& delete_pks,
            const Timestamp* timestamps,
            std::function<void(const SegOffset offset, const Timestamp ts)>
                cb) {
            std::vector<std::pair<SegOffset, Timestamp>> matches;
            for (size_t i = 0; i < delete_pks.size(); ++i) {
                auto delete_pk = std::get<int64_t>(delete_pks[i]);
                for (size_t offset = 0; offset < pks.size(); ++offset) {
                    if (pks[offset] == delete_pk &&
                        insert_tss[offset] < timestamps[i]) {
                        matches.emplace_back(SegOffset(offset), timestamps[i]);
                    }
                }
            }
            if (reverse_matches) {
                std::stable_sort(matches.begin(),
                                 matches.end(),
                                 [](const auto& a, const auto& b) {
                                     return a.first.get() > b.first.get();
                                 });
            }
            for (const auto& [offset, ts] : matches) {
                cb(offset, ts);
            }
        },
        0);
    record->set_sealed_row_count(pks.size());
    return record;
}

// Memory of a record whose per-row earliest-delete index has been built.
template <bool is_sealed>
int64_t
MemSizeWithEarliestIndex(const DeletedRecord<is_sealed>& record) {
    return record.size() * DELETE_PAIR_SIZE +
           record.deleted_row_count() * EARLIEST_DELETE_TS_ENTRY_SIZE;
}

template <bool is_sealed>
std::vector<bool>
QueryDeletedRows(DeletedRecord<is_sealed>& record,
                 int64_t row_count,
                 Timestamp query_ts) {
    BitsetType bitset(row_count);
    auto view = bitset.write_view();
    record.Query(view, row_count, query_ts);
    std::vector<bool> deleted(row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        deleted[i] = view[i];
    }
    return deleted;
}

}  // namespace

// Deletes can be replayed out of timestamp order, e.g. a sealed segment's
// deltalogs (delete@200) are loaded before a remaining L0 segment forwards
// delete@100 for the same row. The row must be deleted from ts 100 on.
TEST(DeleteMVCC, OutOfOrderPushKeepsEarliestDeleteTimestamp) {
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> delete_pk = {int64_t(1)};
    std::vector<Timestamp> later_ts = {200};
    std::vector<Timestamp> earlier_ts = {100};
    record->LoadPush(delete_pk, later_ts.data());
    record->LoadPush(delete_pk, earlier_ts.data());

    EXPECT_EQ(QueryDeletedRows(*record, 2, 99), std::vector<bool>({0, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 100), std::vector<bool>({1, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 150), std::vector<bool>({1, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 250), std::vector<bool>({1, 0}));
    EXPECT_EQ(record->size(), 2);
    EXPECT_EQ(record->deleted_row_count(), 1);
    EXPECT_EQ(record->mem_size(), MemSizeWithEarliestIndex(*record));
}

TEST(DeleteMVCC, UnsortedBatchKeepsEarliestDeleteTimestamp) {
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> delete_pks = {int64_t(1), int64_t(1)};
    std::vector<Timestamp> delete_tss = {200, 100};
    record->LoadPush(delete_pks, delete_tss.data());

    EXPECT_EQ(QueryDeletedRows(*record, 2, 99), std::vector<bool>({0, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 150), std::vector<bool>({1, 0}));
    // the batch is sorted by timestamp, so the later delete is skipped
    EXPECT_EQ(record->size(), 1);
    EXPECT_EQ(record->deleted_row_count(), 1);
    EXPECT_EQ(record->mem_size(), DELETE_PAIR_SIZE);
}

// Sorting an unsorted batch keeps every delete with its own pk.
TEST(DeleteMVCC, UnsortedBatchKeepsEachDeleteWithItsPk) {
    std::vector<int64_t> pks = {1, 2, 3, 4, 5};
    std::vector<Timestamp> insert_tss = {5, 5, 5, 5, 5};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> delete_pks = {
        int64_t(1), int64_t(2), int64_t(3), int64_t(4), int64_t(5)};
    std::vector<Timestamp> delete_tss = {50, 10, 40, 20, 30};
    record->LoadPush(delete_pks, delete_tss.data());

    for (Timestamp ts : {9, 10, 20, 30, 40, 50}) {
        auto deleted = QueryDeletedRows(*record, 5, ts);
        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ(deleted[i], delete_tss[i] <= ts)
                << "pk " << pks[i] << " at ts " << ts;
        }
    }

    // the same with VARCHAR pks, which the sort moves
    std::vector<std::string> str_pks = {"a", "b", "c", "d", "e"};
    DeletedRecord<true> str_record(
        nullptr,
        [&str_pks](
            const std::vector<PkType>& delete_pks,
            const Timestamp* timestamps,
            std::function<void(const SegOffset offset, const Timestamp ts)>
                cb) {
            for (size_t i = 0; i < delete_pks.size(); ++i) {
                auto it = std::find(str_pks.begin(),
                                    str_pks.end(),
                                    std::get<std::string>(delete_pks[i]));
                cb(SegOffset(it - str_pks.begin()), timestamps[i]);
            }
        },
        0);
    str_record.set_sealed_row_count(5);
    std::vector<PkType> str_delete_pks(str_pks.begin(), str_pks.end());
    str_record.LoadPush(str_delete_pks, delete_tss.data());
    for (Timestamp ts : {9, 10, 20, 30, 40, 50}) {
        auto deleted = QueryDeletedRows(str_record, 5, ts);
        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ(deleted[i], delete_tss[i] <= ts)
                << "pk " << str_pks[i] << " at ts " << ts;
        }
    }
}

// An in-order stream must not need the per-row index even when the segment
// reports matches out of the batch's timestamp order.
TEST(DeleteMVCC, InOrderBatchWithUnorderedMatchesBuildsNoIndex) {
    std::vector<int64_t> pks = {2, 1};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss, true);

    std::vector<PkType> first_pk = {int64_t(2)};
    std::vector<Timestamp> first_ts = {50};
    record->StreamPush(first_pk, first_ts.data());
    // sorted batch; matches come back as pk 1 @200, then pk 2 @100
    std::vector<PkType> batch_pks = {int64_t(2), int64_t(1)};
    std::vector<Timestamp> batch_tss = {100, 200};
    record->StreamPush(batch_pks, batch_tss.data());

    EXPECT_EQ(record->size(), 2);
    EXPECT_EQ(record->mem_size(), 2 * DELETE_PAIR_SIZE);
    EXPECT_EQ(QueryDeletedRows(*record, 2, 60), std::vector<bool>({1, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 200), std::vector<bool>({1, 1}));
}

// Older deletes of a row first deleted in the same push are settled by its
// earliest one there and need no per-row index.
TEST(DeleteMVCC, OlderDeletesOfRowDeletedInSamePushBuildNoIndex) {
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> later_pk = {int64_t(2)};
    std::vector<Timestamp> later_ts = {500};
    record->LoadPush(later_pk, later_ts.data());
    std::vector<PkType> older_pks = {int64_t(1), int64_t(1), int64_t(1)};
    std::vector<Timestamp> older_tss = {100, 200, 300};
    record->LoadPush(older_pks, older_tss.data());

    EXPECT_EQ(record->size(), 2);
    EXPECT_EQ(record->mem_size(), 2 * DELETE_PAIR_SIZE);
    EXPECT_EQ(QueryDeletedRows(*record, 2, 99), std::vector<bool>({0, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 150), std::vector<bool>({1, 0}));
}

// In-order re-deletes of a deleted row (e.g. repeated upserts of one pk)
// must not grow the delete list.
TEST(DeleteMVCC, InOrderRedeleteAddsNoEntry) {
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> delete_pk = {int64_t(1)};
    for (Timestamp ts : {100, 200, 300}) {
        std::vector<Timestamp> delete_ts = {ts};
        record->StreamPush(delete_pk, delete_ts.data());
    }
    std::vector<Timestamp> duplicate_ts = {100};
    record->LoadPush(delete_pk, duplicate_ts.data());

    EXPECT_EQ(record->size(), 1);
    EXPECT_EQ(record->deleted_row_count(), 1);
    EXPECT_EQ(record->mem_size(), DELETE_PAIR_SIZE);
    EXPECT_EQ(QueryDeletedRows(*record, 2, 150), std::vector<bool>({1, 0}));
}

// A delete inserted before the last dumped snapshot cursor is missing from
// that snapshot's bitmap, so queries served from the snapshot must not
// skip it.
TEST(DeleteMVCC, DeleteBeforeSnapshotCursorStaysVisible) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const int N = 10004;
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

    // insert pk=i at ts=i
    std::vector<int64_t> age_data(N);
    std::vector<Timestamp> tss(N);
    for (int i = 0; i < N; ++i) {
        age_data[i] = i;
        tss[i] = i;
        insert_record.insert_pk(age_data[i], i);
    }
    auto insert_offset = insert_record.reserved.fetch_add(N);
    insert_record.timestamps_.set_data_raw(insert_offset, tss.data(), N);
    auto field_data = insert_record.get_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // stream delete pk=i at ts=20000+i for rows [0, DN); the first 10000
    // deletes are dumped into a snapshot at ts 29999
    const int DN = N - 2;
    std::vector<PkType> delete_pks(DN);
    std::vector<Timestamp> delete_tss(DN);
    for (int i = 0; i < DN; ++i) {
        delete_pks[i] = age_data[i];
        delete_tss[i] = 20000 + i;
    }
    delete_record.StreamPush(delete_pks, delete_tss.data());
    ASSERT_EQ(1, delete_record.get_snapshots().size());

    // replay older deletes in one push: row DN-1 is already deleted at ts
    // 30001 and row DN is not deleted; both land before the snapshot cursor,
    // while row DN+1 lands after it
    std::vector<PkType> late_pks = {
        int64_t(DN + 1), int64_t(DN - 1), int64_t(DN)};
    std::vector<Timestamp> late_tss = {40000, 15000, 15000};
    delete_record.LoadPush(late_pks, late_tss.data());

    auto deleted = QueryDeletedRows(delete_record, N, 14999);
    EXPECT_FALSE(deleted[DN - 1]);
    EXPECT_FALSE(deleted[DN]);
    EXPECT_FALSE(deleted[DN + 1]);
    deleted = QueryDeletedRows(delete_record, N, 30000);
    EXPECT_TRUE(deleted[DN - 1]);
    EXPECT_TRUE(deleted[DN]);
    EXPECT_FALSE(deleted[DN + 1]);
    deleted = QueryDeletedRows(delete_record, N, 40000);
    EXPECT_TRUE(deleted[DN - 1]);
    EXPECT_TRUE(deleted[DN]);
    EXPECT_TRUE(deleted[DN + 1]);
}

// Same replay order through the sealed segment load path: the segment's own
// deltalogs and a forwarded L0 delete both reach LoadDeletedRecord.
TEST(DeleteMVCC, SealedSegmentLoadKeepsEarliestDeleteTimestamp) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    // pk i inserted at ts i
    int64_t N = 10;
    auto dataset = DataGen(schema, N);
    auto pks = dataset.get_col<int64_t>(pk);
    auto segment = CreateSealedWithFieldDataLoaded(schema, dataset);

    auto load_delete = [&](int64_t delete_pk, Timestamp ts) {
        auto ids = std::make_unique<IdArray>();
        ids->mutable_int_id()->mutable_data()->Add(delete_pk);
        LoadDeletedRecordInfo info = {&ts, ids.get(), 1};
        segment->LoadDeletedRecord(info);
    };
    load_delete(pks[1], 200);
    load_delete(pks[1], 100);

    auto deleted_rows_at = [&](Timestamp ts) {
        BitsetType bitset(N, false);
        auto view = bitset.write_view();
        segment->mask_with_delete(view, N, ts);
        std::vector<int64_t> rows;
        for (int64_t i = 0; i < N; ++i) {
            if (view[i]) {
                rows.push_back(i);
            }
        }
        return rows;
    };
    EXPECT_EQ(deleted_rows_at(99), std::vector<int64_t>());
    EXPECT_EQ(deleted_rows_at(150), std::vector<int64_t>({1}));
    EXPECT_EQ(deleted_rows_at(250), std::vector<int64_t>({1}));
    EXPECT_EQ(segment->get_deleted_count(), 1);
}

// Through a real sealed segment, PK-sorted or not: several versions of one
// pk and an unsorted batch deleting it several times, on a fresh segment and
// after a later delete.
TEST(DeleteMVCC, SealedSegmentUnsortedBatchKeepsEarliestDeletePerVersion) {
    for (bool sorted_by_pk : {false, true}) {
        for (bool later_delete_first : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "sorted_by_pk=" << sorted_by_pk
                         << " later_delete_first=" << later_delete_first);
            auto schema = std::make_shared<Schema>();
            auto pk = schema->AddDebugField("pk", DataType::INT64);
            schema->set_primary_field_id(pk);
            // three versions per pk: pk i / 3 inserted at ts i
            const int64_t N = 30;
            auto dataset = DataGen(schema, N, 42, 0, 3);
            auto pks = dataset.get_col<int64_t>(pk);
            auto segment = CreateSealedSegment(schema,
                                               empty_index_meta,
                                               0,
                                               SegcoreConfig::default_config(),
                                               sorted_by_pk);
            LoadGeneratedDataIntoSegment(dataset, segment.get());

            const int64_t target = pks[N / 2];
            auto load_deletes = [&](const std::vector<Timestamp>& tss) {
                auto ids = std::make_unique<IdArray>();
                for (size_t i = 0; i < tss.size(); ++i) {
                    ids->mutable_int_id()->mutable_data()->Add(target);
                }
                LoadDeletedRecordInfo info = {
                    tss.data(), ids.get(), int64_t(tss.size())};
                segment->LoadDeletedRecord(info);
            };
            if (later_delete_first) {
                load_deletes({60});
            }
            load_deletes({50, 20, 35});

            std::vector<int64_t> versions;
            for (int64_t i = 0; i < N; ++i) {
                if (pks[i] == target) {
                    versions.push_back(i);
                }
            }
            ASSERT_EQ(versions.size(), 3);
            EXPECT_EQ(segment->get_deleted_count(), 3);
            for (Timestamp ts : {19, 20, 35, 60}) {
                BitsetType bitset(N, false);
                auto view = bitset.write_view();
                segment->mask_with_delete(view, N, ts);
                EXPECT_EQ(bitset.count(), ts >= 20 ? 3 : 0) << "at ts " << ts;
                for (auto row : versions) {
                    EXPECT_EQ(bool(view[row]), ts >= 20)
                        << "row " << row << " at ts " << ts;
                }
            }
        }
    }
}

// Re-forwarding deletes that were already applied (e.g. a delete buffer
// replayed through LoadPush after streaming) must not add entries.
TEST(DeleteMVCC, ReplayOfAppliedDeletesAddsNoEntry) {
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    // repeated upserts of pk 1, then a later delete of pk 2
    std::vector<PkType> delete_pk = {int64_t(1)};
    for (Timestamp ts : {100, 110, 120}) {
        std::vector<Timestamp> delete_ts = {ts};
        record->StreamPush(delete_pk, delete_ts.data());
    }
    std::vector<PkType> later_pk = {int64_t(2)};
    std::vector<Timestamp> later_ts = {1000};
    record->StreamPush(later_pk, later_ts.data());

    std::vector<PkType> replay_pks = {int64_t(1), int64_t(1), int64_t(1)};
    std::vector<Timestamp> replay_tss = {120, 110, 100};
    record->LoadPush(replay_pks, replay_tss.data());

    EXPECT_EQ(record->size(), 2);
    EXPECT_EQ(record->deleted_row_count(), 2);
    EXPECT_EQ(QueryDeletedRows(*record, 2, 105), std::vector<bool>({1, 0}));
}

// Only deletes earlier than a row's current earliest delete are recorded
// when older deletes arrive after a newer one.
TEST(DeleteMVCC, OutOfOrderReplayRecordsOnlyEarlierDeletes) {
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> delete_pk = {int64_t(1)};
    std::vector<Timestamp> newest_ts = {500};
    record->LoadPush(delete_pk, newest_ts.data());

    std::vector<PkType> older_pks = {int64_t(1), int64_t(1), int64_t(1)};
    std::vector<Timestamp> older_tss = {100, 150, 120};
    record->LoadPush(older_pks, older_tss.data());

    EXPECT_EQ(record->size(), 2);
    EXPECT_EQ(record->deleted_row_count(), 1);
    EXPECT_EQ(QueryDeletedRows(*record, 2, 99), std::vector<bool>({0, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 100), std::vector<bool>({1, 0}));

    // a later push of a delete between the row's new and old earliest
    // deletes adds nothing
    std::vector<Timestamp> between_ts = {120};
    record->LoadPush(delete_pk, between_ts.data());
    EXPECT_EQ(record->size(), 2);

    // a row first deleted after the per-row index exists, then replayed
    // with older deletes
    std::vector<PkType> other_pk = {int64_t(2)};
    std::vector<Timestamp> other_ts = {300};
    record->LoadPush(other_pk, other_ts.data());
    std::vector<PkType> other_pks = {int64_t(2), int64_t(2)};
    std::vector<Timestamp> other_tss = {250, 200};
    record->LoadPush(other_pks, other_tss.data());

    EXPECT_EQ(record->size(), 4);
    EXPECT_EQ(record->deleted_row_count(), 2);
    EXPECT_EQ(QueryDeletedRows(*record, 2, 199), std::vector<bool>({1, 0}));
    EXPECT_EQ(QueryDeletedRows(*record, 2, 200), std::vector<bool>({1, 1}));
}

// The latest-delete snapshot answers queries at or after its timestamp from
// the delete mask, so it must cover every applied delete whatever the push
// order or push kind.
TEST(DeleteMVCC, LatestSnapshotCoversOutOfOrderPushes) {
    auto saved = ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.load();
    auto restore = folly::makeGuard(
        [&] { ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(saved); });
    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(true);
    std::vector<int64_t> pks = {1, 2, 3, 4};
    std::vector<Timestamp> insert_tss = {10, 10, 10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    auto stream_push = [&](int64_t pk, Timestamp ts) {
        std::vector<PkType> delete_pk = {pk};
        std::vector<Timestamp> delete_ts = {ts};
        record->StreamPush(delete_pk, delete_ts.data());
    };
    auto load_push = [&](int64_t pk, Timestamp ts) {
        std::vector<PkType> delete_pk = {pk};
        std::vector<Timestamp> delete_ts = {ts};
        record->LoadPush(delete_pk, delete_ts.data());
    };

    stream_push(1, 100);
    // a load push after streaming, newer than the stream
    load_push(2, 200);
    EXPECT_EQ(QueryDeletedRows(*record, 4, 300),
              std::vector<bool>({1, 1, 0, 0}));
    // a load push after streaming, older than the stream
    load_push(3, 50);
    EXPECT_EQ(QueryDeletedRows(*record, 4, 150),
              std::vector<bool>({1, 0, 1, 0}));
    // a stream push older than an earlier one
    stream_push(4, 120);
    EXPECT_EQ(QueryDeletedRows(*record, 4, 130),
              std::vector<bool>({1, 0, 1, 1}));
    EXPECT_EQ(QueryDeletedRows(*record, 4, 300),
              std::vector<bool>({1, 1, 1, 1}));
    // the fast path stays available after the pushes
    auto latest = std::atomic_load(&record->latest_snapshot_);
    ASSERT_NE(latest, nullptr);
    EXPECT_EQ(latest->max_ts, 200);
}

// With the optimization turned off at runtime, a completed push drops the
// latest snapshot instead of leaving a stale one for Query.
TEST(DeleteMVCC, LatestSnapshotDroppedWhenOptimizationTurnedOff) {
    auto saved = ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.load();
    auto restore = folly::makeGuard(
        [&] { ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(saved); });
    std::vector<int64_t> pks = {1, 2};
    std::vector<Timestamp> insert_tss = {10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(true);
    std::vector<PkType> first_pk = {int64_t(1)};
    std::vector<Timestamp> first_ts = {100};
    record->StreamPush(first_pk, first_ts.data());
    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(false);
    std::vector<PkType> second_pk = {int64_t(2)};
    std::vector<Timestamp> second_ts = {200};
    record->StreamPush(second_pk, second_ts.data());

    EXPECT_EQ(QueryDeletedRows(*record, 2, 300), std::vector<bool>({1, 1}));
}

// Growing segment: deleted count stays the number of deleted rows when an
// older delete adds a second entry for a row.
TEST(DeleteMVCC, GrowingSegmentOutOfOrderDeleteKeepsRowCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    // pk i inserted at ts i
    int64_t N = 10;
    auto dataset = DataGen(schema, N);
    auto pks = dataset.get_col<int64_t>(pk);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    auto delete_pk = [&](int64_t delete_pk, Timestamp ts) {
        auto ids = std::make_unique<IdArray>();
        ids->mutable_int_id()->mutable_data()->Add(delete_pk);
        ASSERT_TRUE(segment->Delete(1, ids.get(), &ts).ok());
    };
    delete_pk(pks[1], 200);
    delete_pk(pks[1], 100);

    EXPECT_EQ(segment->get_deleted_count(), 1);
    BitsetType bitset(N, false);
    auto view = bitset.write_view();
    segment->mask_with_delete(view, N, 150);
    EXPECT_TRUE(view[1]);
    EXPECT_EQ(bitset.count(), 1);

    // an unsorted batch on a fresh growing segment
    auto fresh = CreateGrowingSegment(schema, empty_index_meta);
    fresh->Insert(0,
                  N,
                  dataset.row_ids_.data(),
                  dataset.timestamps_.data(),
                  std::make_shared<InsertRecordProto>(*dataset.raw_));
    auto ids = std::make_unique<IdArray>();
    std::vector<Timestamp> tss = {50, 20, 35};
    for (size_t i = 0; i < tss.size(); ++i) {
        ids->mutable_int_id()->mutable_data()->Add(pks[2]);
    }
    LoadDeletedRecordInfo info = {tss.data(), ids.get(), int64_t(tss.size())};
    fresh->LoadDeletedRecord(info);
    for (Timestamp ts : {19, 20}) {
        BitsetType fresh_bitset(N, false);
        auto fresh_view = fresh_bitset.write_view();
        fresh->mask_with_delete(fresh_view, N, ts);
        EXPECT_EQ(bool(fresh_view[2]), ts >= 20) << "at ts " << ts;
    }
}

// Upsert history of one pk replayed in one batch: every delete matches all
// earlier versions, and each version keeps exactly its first delete.
TEST(DeleteMVCC, UpsertHistoryInOneBatchKeepsEarliestDeletePerVersion) {
    // version i of pk 1 is inserted at ts 10 * (i + 1); the upsert that
    // inserts version i + 1 deletes all earlier versions at its ts
    const int n = 300;
    std::vector<int64_t> pks(n, 1);
    std::vector<Timestamp> insert_tss(n);
    for (int i = 0; i < n; ++i) {
        insert_tss[i] = 10 * (i + 1);
    }
    auto record = MakeSealedDeletedRecord(pks, insert_tss);

    std::vector<PkType> delete_pks(n - 1, int64_t(1));
    std::vector<Timestamp> delete_tss(insert_tss.begin() + 1, insert_tss.end());
    record->LoadPush(delete_pks, delete_tss.data());

    EXPECT_EQ(record->size(), n - 1);
    EXPECT_EQ(record->deleted_row_count(), n - 1);
    EXPECT_EQ(record->mem_size(), (n - 1) * DELETE_PAIR_SIZE);
    for (Timestamp query_ts : {15, 20, 25, 1505, 3000}) {
        auto deleted = QueryDeletedRows(*record, n, query_ts);
        for (int i = 0; i < n; ++i) {
            EXPECT_EQ(deleted[i], i < n - 1 && insert_tss[i + 1] <= query_ts)
                << "version " << i << " at ts " << query_ts;
        }
    }
}

// When matches come back row by row, a push can insert an entry after the
// last snapshot cursor before one that lands before it; the later one must
// still discard the snapshots.
TEST(DeleteMVCC, RowOrderedMatchesInvalidateSnapshotsBeforeCursor) {
    auto saved_batch_size = DELETE_DUMP_BATCH_SIZE.load();
    auto restore = folly::makeGuard(
        [&] { DELETE_DUMP_BATCH_SIZE.store(saved_batch_size); });
    DELETE_DUMP_BATCH_SIZE.store(2);
    std::vector<int64_t> pks = {1, 2, 3, 4, 5};
    std::vector<Timestamp> insert_tss = {10, 10, 10, 10, 10};
    auto record = MakeSealedDeletedRecord(pks, insert_tss, true);

    // three deletes dump one snapshot at ts 101 with cursor (102, row 2)
    std::vector<PkType> stream_pks = {int64_t(1), int64_t(2), int64_t(3)};
    std::vector<Timestamp> stream_tss = {100, 101, 102};
    record->StreamPush(stream_pks, stream_tss.data());
    ASSERT_EQ(record->get_snapshots().size(), 1);

    // row 4 @200 is applied before row 3 @50, which lands before the cursor
    std::vector<PkType> load_pks = {int64_t(4), int64_t(5)};
    std::vector<Timestamp> load_tss = {50, 200};
    record->LoadPush(load_pks, load_tss.data());

    EXPECT_EQ(QueryDeletedRows(*record, 5, 101),
              std::vector<bool>({1, 1, 0, 1, 0}));
}

// A push that throws part way (e.g. a chunk fails to load during the PK
// search) and is then retried must leave queries and counters right.
TEST(DeleteMVCC, RetryAfterPushThrowsPartWay) {
    auto saved = ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.load();
    auto restore = folly::makeGuard(
        [&] { ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(saved); });
    ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(true);
    // rows A, B, C have pks 1, 2, 3
    bool fail_after_first_match = false;
    DeletedRecord<true> record(
        nullptr,
        [&](const std::vector<PkType>& pks,
            const Timestamp* timestamps,
            std::function<void(const SegOffset offset, const Timestamp ts)>
                cb) {
            for (size_t i = 0; i < pks.size(); ++i) {
                cb(SegOffset(std::get<int64_t>(pks[i]) - 1), timestamps[i]);
                if (fail_after_first_match) {
                    throw std::runtime_error("chunk load failed");
                }
            }
        },
        0);
    record.set_sealed_row_count(3);

    std::vector<PkType> first_pks = {int64_t(1), int64_t(3)};
    std::vector<Timestamp> first_tss = {100, 500};
    record.LoadPush(first_pks, first_tss.data());

    std::vector<PkType> pks = {int64_t(2)};
    std::vector<Timestamp> tss = {200};
    fail_after_first_match = true;
    EXPECT_ANY_THROW(record.LoadPush(pks, tss.data()));
    // B's delete was applied before the failure
    EXPECT_EQ(QueryDeletedRows(record, 3, 600), std::vector<bool>({1, 1, 1}));
    fail_after_first_match = false;
    record.LoadPush(pks, tss.data());

    EXPECT_EQ(QueryDeletedRows(record, 3, 600), std::vector<bool>({1, 1, 1}));
    EXPECT_EQ(QueryDeletedRows(record, 3, 150), std::vector<bool>({1, 0, 0}));
    EXPECT_EQ(record.deleted_row_count(), 3);
}

// Random LoadPush/StreamPush sequences (unsorted batches, duplicates, several
// versions per pk, both match orders, fast path on and off, small dump
// batches) against a model of each row's earliest delete.
TEST(DeleteMVCC, RandomPushesMatchReferenceModel) {
    auto saved_fast_path = ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.load();
    auto saved_batch_size = DELETE_DUMP_BATCH_SIZE.load();
    auto restore = folly::makeGuard([&] {
        ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(saved_fast_path);
        DELETE_DUMP_BATCH_SIZE.store(saved_batch_size);
    });
    for (uint32_t seed = 0; seed < 400; ++seed) {
        std::mt19937 rng(seed);
        ENABLE_LATEST_DELETE_SNAPSHOT_OPTIMIZATION.store(rng() % 2);
        DELETE_DUMP_BATCH_SIZE.store(1 + rng() % 3);
        const int rows = 1 + rng() % 12;
        const int num_pks = 1 + rng() % 4;
        std::vector<int64_t> pks(rows);
        std::vector<Timestamp> insert_tss(rows);
        for (int r = 0; r < rows; ++r) {
            pks[r] = rng() % num_pks;
            insert_tss[r] = 1 + rng() % 30;
        }
        auto record = MakeSealedDeletedRecord(pks, insert_tss, rng() % 2);

        std::vector<Timestamp> earliest(rows, 0);
        Timestamp max_ts = 0;
        const int pushes = 1 + rng() % 10;
        for (int push = 0; push < pushes; ++push) {
            std::vector<PkType> delete_pks;
            std::vector<Timestamp> delete_tss;
            const int n = 1 + rng() % 6;
            for (int i = 0; i < n; ++i) {
                int64_t pk = rng() % num_pks;
                Timestamp ts = 1 + rng() % 80;
                delete_pks.push_back(pk);
                delete_tss.push_back(ts);
                max_ts = std::max(max_ts, ts);
                for (int r = 0; r < rows; ++r) {
                    if (pks[r] == pk && insert_tss[r] < ts &&
                        (earliest[r] == 0 || ts < earliest[r])) {
                        earliest[r] = ts;
                    }
                }
            }
            if (rng() % 2) {
                record->LoadPush(delete_pks, delete_tss.data());
            } else {
                record->StreamPush(delete_pks, delete_tss.data());
            }

            for (Timestamp query_ts = 0; query_ts <= max_ts + 1; ++query_ts) {
                auto deleted = QueryDeletedRows(*record, rows, query_ts);
                for (int r = 0; r < rows; ++r) {
                    ASSERT_EQ(deleted[r],
                              earliest[r] != 0 && earliest[r] <= query_ts)
                        << "seed " << seed << " push " << push << " row " << r
                        << " at ts " << query_ts;
                }
            }
            ASSERT_EQ(record->deleted_row_count(),
                      std::count_if(earliest.begin(),
                                    earliest.end(),
                                    [](Timestamp ts) { return ts != 0; }))
                << "seed " << seed << " push " << push;
        }
    }
}
