#include <cstring>

#include "SegmentBase.h"
#include "segment_c.h"
#include "Partition.h"


CSegmentBase
NewSegment(CPartition partition, unsigned long segment_id) {
  auto p = (milvus::dog_segment::Partition*)partition;

  auto segment = milvus::dog_segment::CreateSegment(p->get_schema());

  segment->set_segment_id(segment_id);

  // TODO: delete print
  std::cout << "create segment " << segment_id << std::endl;
  return (void*)segment.release();
}


void
DeleteSegment(CSegmentBase segment) {
  auto s = (milvus::dog_segment::SegmentBase*)segment;

  // TODO: delete print
  std::cout << "delete segment " << s->get_segment_id() << std::endl;
  delete s;
}

//////////////////////////////////////////////////////////////////

int
Insert(CSegmentBase c_segment,
           signed long int size,
           const unsigned long* primary_keys,
           const unsigned long* timestamps,
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


int
Delete(CSegmentBase c_segment,
           long size,
           const unsigned long* primary_keys,
           const unsigned long* timestamps) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;

  auto res = segment->Delete(size, primary_keys, timestamps);
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


unsigned long
GetTimeBegin(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto time_begin = segment->get_time_begin();
  return time_begin;
}


void
SetTimeBegin(CSegmentBase c_segment, unsigned long time_begin) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  segment->set_time_begin(time_begin);
}


unsigned long
GetTimeEnd(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto time_end = segment->get_time_end();
  return time_end;
}

void
SetTimeEnd(CSegmentBase c_segment, unsigned long time_end) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  segment->set_time_end(time_end);
}


unsigned long
GetSegmentId(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto segment_id = segment->get_segment_id();
  return segment_id;
}

void
SetSegmentId(CSegmentBase c_segment, unsigned long segment_id) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  segment->set_segment_id(segment_id);
}
