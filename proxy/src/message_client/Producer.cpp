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

    Result MsgProducer::send(milvus::grpc::InsertOrDeleteMsg &msg) {
      int32_t channel_id = makeHash(std::to_string(msg.uid())) % 1024;
//      std::cout << "partition id := " << channel_id <<std::endl;
      msg.set_channel_id(channel_id);
      auto msg_str = msg.SerializeAsString();
      return send(msg_str, msg.uid());
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