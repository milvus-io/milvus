#include "SegmentBase.h"
#include "segment_c.h"

CSegmentBase
SegmentBaseInit(unsigned long segment_id) {
  std::cout << "Hello milvus" << std::endl;
  auto seg = milvus::dog_segment::CreateSegment();
  seg->set_segment_id(segment_id);
  return (void*)seg;
}

//int32_t Insert(CSegmentBase c_segment, signed long int size, const unsigned long* primary_keys, const unsigned long int* timestamps, DogDataChunk values) {
//  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
//  milvus::dog_segment::DogDataChunk dataChunk{};
//
//  dataChunk.raw_data = values.raw_data;
//  dataChunk.sizeof_per_row = values.sizeof_per_row;
//  dataChunk.count = values.count;
//
//  auto res = segment->Insert(size, primary_keys, timestamps, dataChunk);
//  return res.code();
//}

int Insert(CSegmentBase c_segment,
               signed long int size,
               const unsigned long* primary_keys,
               const unsigned long int* timestamps,
               void* raw_data,
               int sizeof_per_row,
               signed long int count) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  milvus::dog_segment::DogDataChunk dataChunk{};

  dataChunk.raw_data = raw_data;
  dataChunk.sizeof_per_row = sizeof_per_row;
  dataChunk.count = count;

  auto res = segment->Insert(size, primary_keys, timestamps, dataChunk);
  return res.code();
}

