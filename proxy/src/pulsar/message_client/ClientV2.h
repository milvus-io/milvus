#pragma once

#include "src/utils/Status.h"
#include "Producer.h"
#include "Consumer.h"
#include "grpc/gen-milvus/suvlim.pb.h"

namespace milvus::message_client {
    class MsgClientV2 {
    public:
        MsgClientV2(int64_t client_id, std::string &service_url, const pulsar::ClientConfiguration& config = pulsar::ClientConfiguration());

        ~MsgClientV2();

        // When using MsgClient, make sure it init successfully
        Status Init(const std::string &mut_topic,
                    const std::string &query_topic, const std::string &result_topic);

        // unpackage batch insert or delete request, and delivery message to pulsar per row
        Status SendMutMessage(const milvus::grpc::InsertParam &request);

        Status SendMutMessage(const milvus::grpc::DeleteByIDParam &request);

        //
        Status SendQueryMessage(const milvus::grpc::SearchParam &request);

        void GetQueryResult(int64_t query_id);

    private:
        int64_t GetUniqueQId() {
            return q_id_.fetch_add(1);
        }

    private:
        std::atomic<int64_t> q_id_ = 0;
        int64_t client_id_;
        std::string service_url_;
        std::shared_ptr<MsgConsumer> consumer_;
        std::shared_ptr<MsgProducer> mut_producer_;
        std::shared_ptr<MsgProducer> query_producer_;
    };
}