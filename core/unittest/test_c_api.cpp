#include <iostream>
#include <string>
#include <random>
#include <gtest/gtest.h>

#include "dog_segment/collection_c.h"
#include "dog_segment/segment_c.h"




TEST(CApiTest, CollectionTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  DeleteCollection(collection);
}


TEST(CApiTest, PartitonTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  DeleteCollection(collection);
  DeletePartition(partition);
}


TEST(CApiTest, SegmentTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);
  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}


TEST(CApiTest, InsertTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);

  std::vector<char> raw_data;
  std::vector<uint64_t> timestamps;
  std::vector<int64_t> uids;
  int N = 10000;
  std::default_random_engine e(67);
  for(int i = 0; i < N; ++i) {
    uids.push_back(100000 + i);
    timestamps.push_back(0);
    // append vec
    float vec[16];
    for(auto &x: vec) {
      x = e() % 2000 * 0.001 - 1.0;
    }
    raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
    int age = e() % 100;
    raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
  }

  auto line_sizeof = (sizeof(int) + sizeof(float) * 16);

  auto offset = PreInsert(segment, N);

  auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);

  assert(res == 0);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}


TEST(CApiTest, DeleteTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);

  long delete_primary_keys[] = {100000, 100001, 100002};
  unsigned long delete_timestamps[] = {0, 0, 0};

  auto offset = PreDelete(segment, 3);

  auto del_res = Delete(segment, offset, 3, delete_primary_keys, delete_timestamps);
  assert(del_res == 0);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}



TEST(CApiTest, SearchTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);

  std::vector<char> raw_data;
  std::vector<uint64_t> timestamps;
  std::vector<int64_t> uids;
  int N = 10000;
  std::default_random_engine e(67);
  for(int i = 0; i < N; ++i) {
    uids.push_back(100000 + i);
    timestamps.push_back(0);
    // append vec
    float vec[16];
    for(auto &x: vec) {
      x = e() % 2000 * 0.001 - 1.0;
    }
    raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
    int age = e() % 100;
    raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
  }

  auto line_sizeof = (sizeof(int) + sizeof(float) * 16);

  auto offset = PreInsert(segment, N);

  auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
  assert(ins_res == 0);

  long result_ids[10];
  float result_distances[10];
  auto sea_res = Search(segment, nullptr, 1, result_ids, result_distances);
  assert(sea_res == 0);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}


TEST(CApiTest, IsOpenedTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);

  auto is_opened = IsOpened(segment);
  assert(is_opened);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}


TEST(CApiTest, CloseTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);

  auto status = Close(segment);
  assert(status == 0);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}


namespace {
auto generate_data(int N) {
    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine er(42);
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    std::default_random_engine ei(42);
    for(int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for(auto &x: vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = ei() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}


TEST(CApiTest, TestQuery) {
    auto collection_name = "collection0";
    auto schema_tmp_conf = "null_schema";
    auto collection = NewCollection(collection_name, schema_tmp_conf);
    auto partition_name = "partition0";
    auto partition = NewPartition(collection, partition_name);
    auto segment = NewSegment(partition, 0);


    int N = 1000 * 1000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
    auto offset = PreInsert(segment, N);
    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(res == 0);

    auto row_count = GetRowCount(segment);
    assert(row_count == N);

    std::vector<long> result_ids(10);
    std::vector<float> result_distances(10);
    auto sea_res = Search(segment, nullptr, 1, result_ids.data(), result_distances.data());

    ASSERT_EQ(sea_res, 0);
    ASSERT_EQ(result_ids[0], 10 * N);
    ASSERT_EQ(result_distances[0], 0);

    auto N_del = N / 2;
    std::vector<uint64_t> del_ts(N_del, 100);
    auto pre_off = PreDelete(segment,  N_del);
    Delete(segment, pre_off, N_del, uids.data(), del_ts.data());

    Close(segment);
    BuildIndex(segment);


    std::vector<long> result_ids2(10);
    std::vector<float> result_distances2(10);
    sea_res = Search(segment, nullptr, 104, result_ids2.data(), result_distances2.data());

    std::cout << "case 1" << std::endl;
    for(int i = 0; i < 10; ++i) {
        std::cout << result_ids[i] << "->" << result_distances[i] << std::endl;
    }
    std::cout << "case 2" << std::endl;
    for(int i = 0; i < 10; ++i) {
        std::cout << result_ids2[i] << "->" << result_distances2[i] << std::endl;
    }

    for(auto x: result_ids2) {
        ASSERT_GE(x, 10 * N  + N_del);
        ASSERT_LT(x, 10 * N + N);
    }

//    auto iter = 0;
//    for(int i = 0; i < result_ids.size(); ++i) {
//        auto uid = result_ids[i];
//        auto dis = result_distances[i];
//        if(uid >= 10 * N + N_del) {
//            auto uid2 = result_ids2[iter];
//            auto dis2 = result_distances2[iter];
//            ASSERT_EQ(uid, uid2);
//            ASSERT_EQ(dis, dis2);
//            ++iter;
//        }
//    }


    DeleteCollection(collection);
    DeletePartition(partition);
    DeleteSegment(segment);
}

TEST(CApiTest, GetDeletedCountTest) {
  auto collection_name = "collection0";
  auto schema_tmp_conf = "null_schema";
  auto collection = NewCollection(collection_name, schema_tmp_conf);
  auto partition_name = "partition0";
  auto partition = NewPartition(collection, partition_name);
  auto segment = NewSegment(partition, 0);

  long delete_primary_keys[] = {100000, 100001, 100002};
  unsigned long delete_timestamps[] = {0, 0, 0};

  auto offset = PreDelete(segment, 3);

  auto del_res = Delete(segment, offset, 3, delete_primary_keys, delete_timestamps);
  assert(del_res == 0);

  // TODO: assert(deleted_count == len(delete_primary_keys))
  auto deleted_count = GetDeletedCount(segment);
  assert(deleted_count == 0);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}


TEST(CApiTest, GetRowCountTest) {
    auto collection_name = "collection0";
    auto schema_tmp_conf = "null_schema";
    auto collection = NewCollection(collection_name, schema_tmp_conf);
    auto partition_name = "partition0";
    auto partition = NewPartition(collection, partition_name);
    auto segment = NewSegment(partition, 0);


    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
    auto offset = PreInsert(segment, N);
    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(res == 0);

    auto row_count = GetRowCount(segment);
    assert(row_count == N);

    DeleteCollection(collection);
    DeletePartition(partition);
    DeleteSegment(segment);
}