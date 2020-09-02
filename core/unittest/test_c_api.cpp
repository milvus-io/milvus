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
  std::vector<uint64_t> uids;
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

  auto res = Insert(segment, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);

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

  std::vector<char> raw_data;
  std::vector<uint64_t> timestamps;
  std::vector<uint64_t> uids;
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

  auto ins_res = Insert(segment, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
  assert(ins_res == 0);

  unsigned long delete_primary_keys[] = {100000, 100001, 100002};
  unsigned long delete_timestamps[] = {0, 0, 0};

  auto del_res = Delete(segment, 1, delete_primary_keys, delete_timestamps);
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
  std::vector<uint64_t> uids;
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

  auto ins_res = Insert(segment, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
  assert(ins_res == 0);

  long result_ids;
  float result_distances;
  auto sea_res = Search(segment, nullptr, 0, &result_ids, &result_distances);
  assert(sea_res == 0);
  assert(result_ids == 104490);

  DeleteCollection(collection);
  DeletePartition(partition);
  DeleteSegment(segment);
}
