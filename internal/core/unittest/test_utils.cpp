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
#include <string.h>

#include "common/Utils.h"
#include "query/Utils.h"
#include "test_utils/DataGen.h"

TEST(Util, StringMatch) {
    using namespace milvus;
    using namespace milvus::query;

    ASSERT_ANY_THROW(Match(1, 2, OpType::PrefixMatch));
    ASSERT_ANY_THROW(Match(std::string("not_match_operation"),
                           std::string("not_match"),
                           OpType::LessEqual));

    ASSERT_TRUE(PrefixMatch("prefix1", "prefix"));
    ASSERT_TRUE(PostfixMatch("1postfix", "postfix"));
    ASSERT_TRUE(Match(
        std::string("prefix1"), std::string("prefix"), OpType::PrefixMatch));
    ASSERT_TRUE(Match(
        std::string("1postfix"), std::string("postfix"), OpType::PostfixMatch));

    ASSERT_FALSE(PrefixMatch("", "longer"));
    ASSERT_FALSE(PostfixMatch("", "longer"));

    ASSERT_FALSE(PrefixMatch("dontmatch", "prefix"));
    ASSERT_FALSE(PostfixMatch("dontmatch", "postfix"));
}

TEST(Util, GetDeleteBitmap) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto N = 10;

    InsertRecord insert_record(*schema, N);
    DeletedRecord delete_record;

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
    auto field_data = insert_record.get_field_data_base(i64_fid);
    field_data->set_data_raw(insert_offset, age_data.data(), N);
    insert_record.ack_responder_.AddSegment(insert_offset, insert_offset + N);

    // test case delete pk1(ts = 0) -> insert repeated pk1 (ts = {1 ... N}) -> query (ts = N)
    std::vector<Timestamp> delete_ts = {0};
    std::vector<PkType> delete_pk = {1};
    delete_record.push(delete_pk, delete_ts.data());

    auto query_timestamp = tss[N - 1];
    auto del_barrier = get_barrier(delete_record, query_timestamp);
    auto insert_barrier = get_barrier(insert_record, query_timestamp);
    auto res_bitmap = get_deleted_bitmap(del_barrier,
                                         insert_barrier,
                                         delete_record,
                                         insert_record,
                                         query_timestamp);
    ASSERT_EQ(res_bitmap->bitmap_ptr->count(), 0);

    // test case insert repeated pk1 (ts = {1 ... N}) -> delete pk1 (ts = N) -> query (ts = N)
    delete_ts = {uint64_t(N)};
    delete_pk = {1};
    delete_record.push(delete_pk, delete_ts.data());

    del_barrier = get_barrier(delete_record, query_timestamp);
    res_bitmap = get_deleted_bitmap(del_barrier,
                                    insert_barrier,
                                    delete_record,
                                    insert_record,
                                    query_timestamp);
    ASSERT_EQ(res_bitmap->bitmap_ptr->count(), N - 1);

    // test case insert repeated pk1 (ts = {1 ... N}) -> delete pk1 (ts = N) -> query (ts = N/2)
    query_timestamp = tss[N - 1] / 2;
    del_barrier = get_barrier(delete_record, query_timestamp);
    res_bitmap = get_deleted_bitmap(
        del_barrier, N, delete_record, insert_record, query_timestamp);
    ASSERT_EQ(res_bitmap->bitmap_ptr->count(), 0);
}

TEST(Util, OutOfRange) {
    using milvus::query::out_of_range;

    ASSERT_FALSE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) - 1));
    ASSERT_FALSE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()) + 1));

    ASSERT_TRUE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1));
    ASSERT_TRUE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1));
}
