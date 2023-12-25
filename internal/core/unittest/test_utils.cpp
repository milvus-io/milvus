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
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/Exception.h"
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

TEST(Util, upper_bound) {
    using milvus::Timestamp;
    using milvus::segcore::ConcurrentVector;
    using milvus::segcore::upper_bound;

    std::vector<Timestamp> data{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ConcurrentVector<Timestamp> timestamps(1);
    timestamps.grow_to_at_least(data.size());
    timestamps.set_data(0, data.data(), data.size());

    ASSERT_EQ(1, upper_bound(timestamps, 0, data.size(), 0));
    ASSERT_EQ(5, upper_bound(timestamps, 0, data.size(), 4));
    ASSERT_EQ(10, upper_bound(timestamps, 0, data.size(), 10));
}

// A simple wrapper that removes a temporary file.
struct TmpFileWrapper {
    int fd = -1;
    std::string filename;

    TmpFileWrapper(const std::string& _filename) : filename{_filename} {
        fd = open(filename.c_str(),
                  O_RDWR | O_CREAT | O_EXCL,
                  S_IRUSR | S_IWUSR | S_IXUSR);
    }
    TmpFileWrapper(const TmpFileWrapper&) = delete;
    TmpFileWrapper(TmpFileWrapper&&) = delete;
    TmpFileWrapper&
    operator=(const TmpFileWrapper&) = delete;
    TmpFileWrapper&
    operator=(TmpFileWrapper&&) = delete;
    ~TmpFileWrapper() {
        if (fd != -1) {
            close(fd);
            remove(filename.c_str());
        }
    }
};

TEST(Util, read_from_fd) {
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file = std::string("/tmp/") + uuid_string;

    auto tmp_file = TmpFileWrapper(file);
    ASSERT_NE(tmp_file.fd, -1);

    size_t data_size = 100 * 1024 * 1024;  // 100M
    auto index_data = std::shared_ptr<uint8_t[]>(new uint8_t[data_size]);
    auto max_loop = size_t(INT_MAX) / data_size + 1;  // insert data > 2G
    for (int i = 0; i < max_loop; ++i) {
        auto size_write = write(tmp_file.fd, index_data.get(), data_size);
        ASSERT_GE(size_write, 0);
    }

    auto read_buf =
        std::shared_ptr<uint8_t[]>(new uint8_t[data_size * max_loop]);
    EXPECT_NO_THROW(milvus::index::ReadDataFromFD(
        tmp_file.fd, read_buf.get(), data_size * max_loop));

    // On Linux, read() (and similar system calls) will transfer at most 0x7ffff000 (2,147,479,552) bytes once
    EXPECT_THROW(
        milvus::index::ReadDataFromFD(
            tmp_file.fd, read_buf.get(), data_size * max_loop, INT_MAX),
        milvus::SegcoreError);
}
