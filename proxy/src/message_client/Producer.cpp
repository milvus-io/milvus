
#include "Producer.h"

namespace milvus {
namespace message_client {

    MsgProducer::MsgProducer(std::shared_ptr<MsgClient> &client, const std::string &topic,
                             const ProducerConfiguration& conf) : client_(client), config_(conf) {
        createProducer(topic);
    }

    Result MsgProducer::createProducer(const std::string &topic) {
        return client_->createProducer(topic, producer_);
    }

    Result MsgProducer::send(const Message &msg) {
        return producer_.send(msg);
    }

    Result MsgProducer::send(const std::string &msg) {
        auto pulsar_msg = pulsar::MessageBuilder().setContent(msg).build();
        return send(pulsar_msg);
    }

    Result MsgProducer::send(const milvus::grpc::InsertOrDeleteMsg &msg) {
      auto msg_str = msg.SerializeAsString();
      return send(msg_str);
    }

    Result MsgProducer::send(const milvus::grpc::SearchMsg &msg) {
      auto msg_str = msg.SerializeAsString();
      return send(msg_str);
    }

    Result MsgProducer::send(const milvus::grpc::GetEntityIDsParam &msg) {
      auto msg_str =  msg.SerializeAsString();
      return send(msg_str);
    }

    Result MsgProducer::send(const milvus::grpc::TimeSyncMsg &msg) {
      auto msg_str = msg.SerializeAsString();
      return send(msg_str);
    }

    Result MsgProducer::close() {
        return producer_.close();
    }

}
}