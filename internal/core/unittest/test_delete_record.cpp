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

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/format.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>

#include "segcore/DeletedRecord.h"
#include "segcore/SegmentGrowingImpl.h"

#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

TEST(DeleteMVCC, common_case) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);
    ASSERT_EQ(0, segment->get_real_count());

    // load insert:     pk (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    // with timestamp   ts (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    int64_t c = 10;
    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    SealedLoadFieldData(dataset, *segment);
    ASSERT_EQ(c, segment->get_real_count());
    auto& insert_record = segment->get_insert_record();
    DeletedRecord<true> delete_record(
        &insert_record,
        [&insert_record](const PkType& pk, Timestamp timestamp) {
            return insert_record.search_pk(pk, timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        auto insert_barrier = c;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }
    {
        BitsetType bitsets(c);
        BitsetTypeView bitsets_view(bitsets);
        auto insert_barrier = c;
        // query at ts (11)
        Timestamp query_timestamp = 11;
        // query at ts (11)
        query_timestamp = 11;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        auto insert_barrier = c;
        // query at ts (12)
        Timestamp query_timestamp = 12;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        auto insert_barrier = c;
        // query at ts (14)
        Timestamp query_timestamp = 14;

        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        auto insert_barrier = c;
        // query at ts (14)
        Timestamp query_timestamp = 14;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
        std::vector<bool> expected = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0};
        for (int i = 0; i < c; i++) {
            ASSERT_EQ(bitsets_view[i], expected[i]);
        }
    }
}

TEST(DeleteMVCC, delete_exist_duplicate_pks) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 10;
    uint64_t seg_id = 101;
    InsertRecord insert_record(*schema, N);
    DeletedRecord<false> delete_record(
        &insert_record,
        [&insert_record](const PkType& pk, Timestamp timestamp) {
            return insert_record.search_pk(pk, timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        int64_t insert_barrier = N;
        // query at ts (10)
        Timestamp query_timestamp = 10;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 50000;
    uint64_t seg_id = 101;
    InsertRecord insert_record(*schema, N);
    DeletedRecord<false> delete_record(
        &insert_record,
        [&insert_record](const PkType& pk, Timestamp timestamp) {
            return insert_record.search_pk(pk, timestamp);
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

    auto snapshots = std::move(delete_record.get_snapshots());
    ASSERT_EQ(3, snapshots.size());
    ASSERT_EQ(snapshots[2].second.count(), 30000);
}

TEST(DeleteMVCC, insert_after_snapshot) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 11000;
    uint64_t seg_id = 101;
    InsertRecord insert_record(*schema, N);
    DeletedRecord<false> delete_record(
        &insert_record,
        [&insert_record](const PkType& pk, Timestamp timestamp) {
            return insert_record.search_pk(pk, timestamp);
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

    auto snapshots = std::move(delete_record.get_snapshots());
    ASSERT_EQ(1, snapshots.size());
    ASSERT_EQ(snapshots[0].second.count(), 10000);

    // Query at N+1 ts
    {
        BitsetType bitsets(N);
        BitsetTypeView bitsets_view(bitsets);
        int64_t insert_barrier = N;
        Timestamp query_timestamp = N + 1;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
        BitsetTypeView bitsets_view(bitsets);
        int64_t insert_barrier = N + AN;
        Timestamp query_timestamp = N + AN + 1;
        delete_record.Query(bitsets_view, insert_barrier, query_timestamp);
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 1000000;
    uint64_t seg_id = 101;
    InsertRecord insert_record(*schema, N);
    DeletedRecord<false> delete_record(
        &insert_record,
        [&insert_record](const PkType& pk, Timestamp timestamp) {
            return insert_record.search_pk(pk, timestamp);
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
    BitsetTypeView res_view(res_bitmap);
    start = std::chrono::steady_clock::now();
    delete_record.Query(res_view, insert_barrier, query_timestamp);
    end = std::chrono::steady_clock::now();
    std::cout << "query cost:"
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                     .count()
              << std::endl;
}
