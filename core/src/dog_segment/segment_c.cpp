#include "SegmentBase.h"
#include "segment_c.h"
#include "Partition.h"

CSegmentBase
NewSegment(CPartition partition, unsigned long segment_id) {
  auto p = (milvus::dog_segment::Partition*)partition;

  auto segment = milvus::dog_segment::CreateSegment(p->get_schema());

  segment->set_segment_id(segment_id);

  return (void*)segment.release();
}

void DeleteSegment(CSegmentBase segment) {
  auto s = (milvus::dog_segment::SegmentBase*)segment;

  delete s;
}

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

