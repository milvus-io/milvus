
#include "Consumer.h"
#include "grpc/message.pb.h"

namespace milvus {
namespace message_client {

MsgConsumer::MsgConsumer(std::shared_ptr<MsgClient> &client, std::string subscription_name, const ConsumerConfiguration conf)
        :client_(client), config_(conf), subscription_name_(subscription_name){}

Result MsgConsumer::subscribe(const std::string &topic) {
  return client_->subscribe(topic, subscription_name_, config_, consumer_);
}

Result MsgConsumer::subscribe(const std::vector<std::string> &topics) {
  return client_->subscribe(topics, subscription_name_, config_, consumer_);
}

Result MsgConsumer::unsubscribe() {
  return consumer_.unsubscribe();
}

Result MsgConsumer::receive(Message &msg) {
  return consumer_.receive(msg);
}

Result MsgConsumer::receive(milvus::grpc::QueryResult &res) {
  Message msg;
  auto result = consumer_.receive(msg);
  if (result == pulsar::ResultOk) {
    res.ParseFromString(msg.getDataAsString());
  }
  return result;
}

Result MsgConsumer::receive(milvus::grpc::Entities &res) {
  Message msg;
  auto result = consumer_.receive(msg);
  if (result == pulsar::ResultOk) {
    res.ParseFromString(msg.getDataAsString());
  }
  return result;
}

Result MsgConsumer::receive(milvus::grpc::EntityIds &res) {
  Message msg;
  auto result = consumer_.receive(msg);
  if (result == pulsar::ResultOk) {
    res.ParseFromString(msg.getDataAsString());
  }
  return result;
}

Result MsgConsumer::receive(milvus::grpc::Status &res) {
  Message msg;
  auto result = consumer_.receive(msg);
  if (result == pulsar::ResultOk) {
    res.ParseFromString(msg.getDataAsString());
  }
  return result;
}

Result MsgConsumer::close() {
  return consumer_.close();
}

Result MsgConsumer::acknowledge(const Message &message) {
  return consumer_.acknowledge(message);
}

}
}
