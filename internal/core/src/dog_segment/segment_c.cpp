#include <cstring>

#include "SegmentBase.h"
#include "Collection.h"
#include "segment_c.h"
#include "Partition.h"
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>


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
           long int reserved_offset,
           signed long int size,
           const long* primary_keys,
           const unsigned long* timestamps,
           void* raw_data,
           int sizeof_per_row,
           signed long int count) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  milvus::dog_segment::DogDataChunk dataChunk{};

  dataChunk.raw_data = raw_data;
  dataChunk.sizeof_per_row = sizeof_per_row;
  dataChunk.count = count;

  auto res = segment->Insert(reserved_offset, size, primary_keys, timestamps, dataChunk);

  // TODO: delete print
  // std::cout << "do segment insert, sizeof_per_row = " << sizeof_per_row << std::endl;
  return res.code();
}


long int
PreInsert(CSegmentBase c_segment, long int size) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;

  // TODO: delete print
  // std::cout << "PreInsert segment " << std::endl;
  return segment->PreInsert(size);
}


int
Delete(CSegmentBase c_segment,
           long int reserved_offset,
           long size,
           const long* primary_keys,
           const unsigned long* timestamps) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;

  auto res = segment->Delete(reserved_offset, size, primary_keys, timestamps);
  return res.code();
}


long int
PreDelete(CSegmentBase c_segment, long int size) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;

  // TODO: delete print
  // std::cout << "PreDelete segment " << std::endl;
  return segment->PreDelete(size);
}


//int
//Search(CSegmentBase c_segment,
//           const char*  query_json,
//           unsigned long timestamp,
//           float* query_raw_data,
//           int num_of_query_raw_data,
//           long int* result_ids,
//           float* result_distances) {
//  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
//  milvus::dog_segment::QueryResult query_result;
//
//  // parse query param json
//  auto query_param_json_string = std::string(query_json);
//  auto query_param_json = nlohmann::json::parse(query_param_json_string);
//
//  // construct QueryPtr
//  auto query_ptr = std::make_shared<milvus::query::Query>();
//  query_ptr->num_queries = query_param_json["num_queries"];
//  query_ptr->topK = query_param_json["topK"];
//  query_ptr->field_name = query_param_json["field_name"];
//
//  query_ptr->query_raw_data.resize(num_of_query_raw_data);
//  memcpy(query_ptr->query_raw_data.data(), query_raw_data, num_of_query_raw_data * sizeof(float));
//
//  auto res = segment->Query(query_ptr, timestamp, query_result);
//
//  // result_ids and result_distances have been allocated memory in goLang,
//  // so we don't need to malloc here.
//  memcpy(result_ids, query_result.result_ids_.data(), query_result.row_num_ * sizeof(long int));
//  memcpy(result_distances, query_result.result_distances_.data(), query_result.row_num_ * sizeof(float));
//
//  return res.code();
//}

int
Search(CSegmentBase c_segment,
        CQueryInfo  c_query_info,
        unsigned long timestamp,
        float* query_raw_data,
        int num_of_query_raw_data,
        long int* result_ids,
        float* result_distances) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  milvus::dog_segment::QueryResult query_result;

  // construct QueryPtr
  auto query_ptr = std::make_shared<milvus::query::Query>();
  query_ptr->num_queries = c_query_info.num_queries;
  query_ptr->topK = c_query_info.topK;
  query_ptr->field_name = c_query_info.field_name;

  query_ptr->query_raw_data.resize(num_of_query_raw_data);
  memcpy(query_ptr->query_raw_data.data(), query_raw_data, num_of_query_raw_data * sizeof(float));

  auto res = segment->Query(query_ptr, timestamp, query_result);

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

int
BuildIndex(CCollection c_collection, CSegmentBase c_segment) {
    auto collection = (milvus::dog_segment::Collection*)c_collection;
    auto segment = (milvus::dog_segment::SegmentBase*)c_segment;

    auto status = segment->BuildIndex(collection->get_index());
    return status.code();
}


bool
IsOpened(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto status = segment->get_state();
  return status == milvus::dog_segment::SegmentBase::SegmentState::Open;
}

long int
GetMemoryUsageInBytes(CSegmentBase c_segment) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  auto mem_size = segment->GetMemoryUsageInBytes();
  return mem_size;
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
