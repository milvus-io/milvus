#pragma once

#include "utils/Status.h"
#include "Producer.h"
#include "Consumer.h"
#include "grpc/message.pb.h"

namespace milvus::message_client {
constexpr uint32_t ParallelNum = 12 * 20;

class MsgClientV2 {
 public:
  MsgClientV2(int64_t client_id,
              const std::string &service_url,
              const uint32_t mut_parallelism = ParallelNum,
              const pulsar::ClientConfiguration &config = pulsar::ClientConfiguration());
  ~MsgClientV2();

  // When using MsgClient, make sure it init successfully
  Status Init(const std::string &insert_delete,
              const std::string &search,
              const std::string &time_sync,
              const std::string &search_by_id,
              const std::string &search_result);

  // unpackage batch insert or delete request, and delivery message to pulsar per row
  Status SendMutMessage(const milvus::grpc::InsertParam &request, uint64_t timestamp);

  Status SendMutMessage(const milvus::grpc::DeleteByIDParam &request, uint64_t timestamp);

  //
  Status SendQueryMessage(const milvus::grpc::SearchParam &request, uint64_t timestamp, int64_t &query_id);

  Status GetQueryResult(int64_t query_id, milvus::grpc::QueryResult *result);

 private:
  int64_t GetUniqueQId() {
    return q_id_.fetch_add(1);
  }

 private:
  std::atomic<int64_t> q_id_ = 0;
  int64_t client_id_;
  std::string service_url_;
  std::shared_ptr<MsgConsumer> consumer_;
  // std::shared_ptr<MsgProducer> insert_delete_producer_;
  std::shared_ptr<MsgProducer> search_producer_;
  std::shared_ptr<MsgProducer> time_sync_producer_;
  std::shared_ptr<MsgProducer> search_by_id_producer_;
  std::vector<std::shared_ptr<MsgProducer>> paralle_mut_producers_;
  const uint32_t mut_parallelism_;
};
}