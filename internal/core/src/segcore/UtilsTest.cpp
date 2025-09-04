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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "common/Types.h"
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
