#include <gtest/gtest.h>
#include <omp.h>
#include "message_client/Producer.h"
#include "grpc/message.pb.h"


TEST(CLIENT_CPP, MultiTopic) {
  auto row_count = 10000;
  int ParallelNum = 128;
  uint64_t timestamp = 0;
  // TODO: Get the segment from master
  int64_t segment = 0;
  auto stats = std::vector<pulsar::Result>(ParallelNum);
  std::chrono::milliseconds start = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  std::string blob = new char[2000];
  milvus::grpc::RowData data;
  data.set_blob(blob);
#pragma omp parallel for default(none), shared(row_count, timestamp, segment, stats, data), num_threads(ParallelNum)
  for (auto i = 0; i < row_count; i++) {
    milvus::grpc::InsertOrDeleteMsg mut_msg;
    int this_thread = omp_get_thread_num();
    mut_msg.set_op(milvus::grpc::OpType::INSERT);
    mut_msg.set_uid(i);
    mut_msg.set_client_id(0);
    mut_msg.set_timestamp(timestamp);
    mut_msg.set_collection_name("collection0");
    mut_msg.set_partition_tag("partition0");
    mut_msg.set_segment_id(segment);
//    mut_msg.mutable_rows_data()->CopyFrom(&data);
//    auto result = paralle_mut_producers_[this_thread]->send(mut_msg);
//    if (result != pulsar::ResultOk) {
//      stats[this_thread] = result;
//    }
  }
}