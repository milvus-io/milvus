#include "Producer.h"
#include <string>
#include "M3_hash.h"
#include <iostream>

namespace milvus {
namespace message_client {

    MsgProducer::MsgProducer(std::shared_ptr<MsgClient> &client, const std::string &topic,
                             const ProducerConfiguration& conf) : client_(client), config_(conf) {
        createProducer(topic);
    }

    Result MsgProducer::createProducer(const std::string &topic) {
        return client_->createProducer(topic, config_, producer_);
    }

    Result MsgProducer::send(const Message &msg) {
        return producer_.send(msg);
    }

    void MsgProducer::sendAsync(const Message &msg, pulsar::SendCallback callback) {
      return producer_.sendAsync(msg, callback);
    }

    Result MsgProducer::send(const std::string &msg) {
        auto pulsar_msg = pulsar::MessageBuilder().setContent(msg).build();
        return send(pulsar_msg);
    }

    Result MsgProducer::send(const std::string &msg, const int64_t partitioned_key) {
      auto pulsar_msg = pulsar::MessageBuilder().
              setContent(msg).
              setPartitionKey(std::to_string(partitioned_key)).
              build();
      return send(pulsar_msg);
    }

    void MsgProducer::sendAsync(const std::string &msg, int64_t partitioned_key, pulsar::SendCallback callback) {
      auto pulsar_msg = pulsar::MessageBuilder().
          setContent(msg).
          setPartitionKey(std::to_string(partitioned_key)).
          build();
      return sendAsync(pulsar_msg, callback);
    }

    Result MsgProducer::send(milvus::grpc::InsertOrDeleteMsg &msg) {
      auto msg_str = msg.SerializeAsString();
      return send(msg_str, msg.uid());
    }

    void MsgProducer::sendAsync(milvus::grpc::InsertOrDeleteMsg &msg, pulsar::SendCallback callback) {
      auto msg_str = msg.SerializeAsString();
      return sendAsync(msg_str, msg.uid(), callback);
    }

    Result MsgProducer::send(milvus::grpc::SearchMsg &msg) {
      auto msg_str = msg.SerializeAsString();
      return send(msg_str, msg.uid());
    }

//    Result MsgProducer::send(const milvus::grpc::EntityIdentity &msg) {
//      auto msg_str =  msg.SerializeAsString();
//      return send(msg_str);
//    }

    Result MsgProducer::send(milvus::grpc::TimeSyncMsg &msg) {
      auto msg_str = msg.SerializeAsString();
      return send(msg_str);
    }

    Result MsgProducer::close() {
        return producer_.close();
    }
}
}