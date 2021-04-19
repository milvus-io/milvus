#include <vector>
#include <cstdint>
#include <random>
#include <iostream>
#include "cwrap.h"

//int main() {
//  auto s = SegmentBaseInit();
//
//  std::vector<char> raw_data;
//  std::vector<uint64_t> timestamps;
//  std::vector<uint64_t> uids;
//  int N = 10000;
//  std::default_random_engine e(67);
//  for(int i = 0; i < N; ++i) {
//    uids.push_back(100000 + i);
//    timestamps.push_back(0);
//    // append vec
//    float vec[16];
//    for(auto &x: vec) {
//      x = e() % 2000 * 0.001 - 1.0;
//    }
//    raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
//    int age = e() % 100;
//    raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
//  }
//
//  auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
//
//  DogDataChunk dogDataChunk{};
//  dogDataChunk.count = N;
//  dogDataChunk.raw_data = raw_data.data();
//  dogDataChunk.sizeof_per_row = (int)line_sizeof;
//
//  auto res = Insert(s, N, uids.data(), timestamps.data(), dogDataChunk);
//
//  std::cout << res << std::endl;
//}

int main() {
  auto s = SegmentBaseInit();

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

  auto res = Insert(s, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);

  std::cout << res << std::endl;
}

