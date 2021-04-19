#include <cstring>

#include "SegmentBase.h"
#include "segment_c.h"
#include "Partition.h"


CSegmentBase
NewSegment(CPartition partition, unsigned long segment_id) {
  auto p = (milvus::dog_segment::Partition*)partition;

  auto segment = milvus::dog_segment::CreateSegment(p->get_schema());

  // TODO: delete print
  std::cout << "create segment " << segment_id << std::endl;
  return (void*)segment.release();
}


void
DeleteSegment(CSegmentBase segment) {
  auto s = (milvus::dog_segment::SegmentBase*)segment;

  // TODO: delete print
  std::cout << "delete segment " << std::endl;
  delete s;
}

//////////////////////////////////////////////////////////////////

int
Insert(CSegmentBase c_segment,
       signed long int size,
       const long* primary_keys,
       const unsigned long* timestamps,
       void* raw_data,
       int sizeof_per_row,
       signed long int count,
       unsigned long timestamp_min,
       unsigned long timestamp_max) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  milvus::dog_segment::DogDataChunk dataChunk{};

  dataChunk.raw_data = raw_data;
  dataChunk.sizeof_per_row = sizeof_per_row;
  dataChunk.count = count;

  auto res = segment->Insert(size, primary_keys, timestamps, dataChunk, std::make_pair(timestamp_min, timestamp_max));
  return res.code();
}


int
Delete(CSegmentBase c_segment,
       long size,
       const long* primary_keys,
       const unsigned long* timestamps,
       unsigned long timestamp_min,
       unsigned long timestamp_max) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;

  auto res = segment->Delete(size, primary_keys, timestamps, std::make_pair(timestamp_min, timestamp_max));
  return res.code();
}


int
Search(CSegmentBase c_segment,
           void* fake_query,
           unsigned long timestamp,
           long int* result_ids,
           float* result_distances) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  milvus::dog_segment::QueryResult query_result;

  auto res = segment->Query(nullptr, timestamp, query_result);

  // result_ids and result_distances have been allocated memory in goLang,
  // so we don't need to malloc here.
  memcpy(result_ids, query_result.result_ids_.data(), query_result.row_num_ * sizeof(long int));
  memcpy(result_distances, query_result.result_distances_.data(), query_result.row_num_ * sizeof(float));

  return res.code();
}

//////////////////////////////////////////////////////////////////

int
Close(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto status = segment->Close();
  return status.code();
}


bool
IsOpened(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto status = segment->get_state();
  return status == milvus::dog_segment::SegmentBase::SegmentState::Open;
}

//////////////////////////////////////////////////////////////////

long int
GetRowCount(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto row_count = segment->get_row_count();
  return row_count;
}


long int
GetDeletedCount(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto deleted_count = segment->get_deleted_count();
  return deleted_count;
}
